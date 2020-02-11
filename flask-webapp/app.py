from flask import Flask, abort, redirect, request, render_template, jsonify
from cassandraConnector import CassandraConnector

import datetime
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly
from dash.dependencies import Input, Output
import plotly.graph_objs as go
# specify max size... kind of like array in java... but it pops out if insert is too large
from collections import deque
import random
import pandas as pd
import dash_table
from redisConnector import RedisConnector
import redis
import geohash2
from geoJSON import GeoJSON
import plotly.express as px


server = Flask(__name__)

# df = pd.read_csv('solar.csv')
# example df for columns in dash_table.DataTable
dictDf = {'Geohash': {0: 'v1hsg178bewy', 1: 'gcpuwqxsfh37', 2: 'gcpuxh5mzjxy', 3: 'v1hsfch8kt02', 4: 'gcpuwwy5yjfv', 5: 'gcpuxh30tzgn', 6: 'v1hsff8mcz79', 7: 'gcpuwu4r3dud', 8: 'gcpuwves6qmt', 9: 'v1hsfctwvnu6'}, 'Cumulative Energy': {0: 0.0133, 1: 0.01301, 2: 0.01278, 3: 0.012649999999999998, 4: 0.012570000000000003, 5: 0.012570000000000001, 6: 0.012549999999999997, 7: 0.012459999999999999, 8: 0.01242, 9: 0.012419999999999999}}

df = pd.DataFrame(dictDf)

ALLOWED_TYPES = (
    "text"
)


app = dash.Dash(__name__,
                server=server,
                # routes_pathname_prefix
                url_base_pathname='/dash/'
                )

colors = {
    'background': '#111111',
    'text': '#7FDBFF',
    'thefttext' : '#FF0000'
}

# this must be set to allow the tabs to dynamically update with data from callbacks
app.config['suppress_callback_exceptions'] = True

# connecting to cassandra and creating a session
cc = CassandraConnector()
session = cc.getSession()


# connecting to redis and creating a session (r)
rc = RedisConnector()
r = redis.Redis()

# This is the layout for the dash app, graphs and tables are nested within HTML divs
# graphs and tables are identified with their id's which are used in the call back methods below
app.layout = html.Div([
    html.H1('Tabs'),
    dcc.Tabs(id="tabs-example", value='tab-1-example', children=[
        dcc.Tab(label='Tab One', value='tab-1-example'),
        dcc.Tab(label='Tab Two', value='tab-2-example'),
    ]),
    html.Div(id='tabs-content-example')
])


@app.callback(Output('tabs-content-example', 'children'),
              [Input('tabs-example', 'value')])
def render_content(tab):
    if tab == 'tab-1-example':
        return html.Div([
            html.Div( [
                html.H2('Individual Electricity Usage'),
                        html.Div(id='live-update-text'),
                        html.H3('Insert your User ID here: '),
                        dcc.Input(id='input', value='', type='text'),
                        dcc.Graph(id='live-update-graph'),
                        dcc.Interval(
                        # this runs the method to obtain the data for the graph once every second
                            id='interval-component',
                            interval=1*1000, # in milliseconds
                            n_intervals=0
                        )
                ]),
                    html.Div([
                        dcc.Graph(id='top-ten-graph')

                    ]),
                    html.Div([
                        html.H2('Latest Outages'),
                        dash_table.DataTable(
                            id='outage-table',
                            style_cell={
                            'textAlign': 'center',
                            'width' : '600px',
                            'font_size' : '16px'
                            },
                            columns=[{'name': 'Geohash', 'id': 'Geohash'},
                             {'name': 'Timestamp', 'id': 'Timestamp'}])
                            ]),
                    html.Div([
                        html.H2('Potential Energy Theft'),
                        dash_table.DataTable(
                            id='theft-table',
                            style_cell={
                            'textAlign': 'center',
                            'width' : '600px',
                            'font_size' : '16px'
                            },
                            columns=[{'name': 'Geohash', 'id': 'Geohash'},
                             {'name': 'Timestamp', 'id': 'Timestamp'}])
                            ])
                        ])
    elif tab == 'tab-2-example':
        return html.Div([
            html.Div( [
                html.H2('Community Energy Usage'),
                dcc.Graph(id='choropleth-graph'),
                dcc.Interval(
                    id='interval-component-two',
                    interval=5*1000, # in milliseconds
                    n_intervals=0
                )
            ]),
            html.Div([
            dcc.Link('Navigate to "/page-2"', href='/dash/page-2'),
            ])
        ])



# app.layout = html.Div([
#     html.Div( [
#         html.H2('Individual Electricity Usage'),
#                 html.Div(id='live-update-text'),
#                 html.H3('Insert your User ID here: '),
#                 dcc.Input(id='input', value='', type='text'),
#                 dcc.Graph(id='live-update-graph'),
#                 dcc.Interval(
#                 # this runs the method to obtain the data for the graph once every second
#                     id='interval-component',
#                     interval=1*1000, # in milliseconds
#                     n_intervals=0
#                 )
#         ]),
#             html.Div([
#                 dcc.Graph(id='top-ten-graph')
#
#             ]),
#             html.Div([
#                 html.H2('Latest Outages'),
#                 dash_table.DataTable(
#                     id='outage-table',
#                     style_cell={
#                     'textAlign': 'center',
#                     'width' : '600px',
#                     'font_size' : '16px'
#                     },
#                     columns=[{'name': 'Geohash', 'id': 'Geohash'},
#                      {'name': 'Timestamp', 'id': 'Timestamp'}])
#                     ]),
#             html.Div([
#                 html.H2('Potential Energy Theft'),
#                 dash_table.DataTable(
#                     id='theft-table',
#                     style_cell={
#                     'textAlign': 'center',
#                     'width' : '600px',
#                     'font_size' : '16px'
#                     },
#                     columns=[{'name': 'Geohash', 'id': 'Geohash'},
#                      {'name': 'Timestamp', 'id': 'Timestamp'}])
#                     ]),
#         html.Div( [
#             html.H2('Community Energy Usage'),
#             dcc.Graph(id='choropleth-graph'),
#             dcc.Interval(
#                 id='interval-component-two',
#                 interval=5*1000, # in milliseconds
#                 n_intervals=0
#             )
#         ]),
#         html.Div([
#         dcc.Link('Navigate to "/page-2"', href='/dash/page-2'),
#         ])
#     ])


# Multiple components can update everytime interval gets fired.
# this handles user input and individual time series graph
@app.callback(Output('live-update-graph', 'figure'),
              [Input('interval-component', 'n_intervals'),
              Input('input', 'value')])
def update_graph_live(n, value):


    x_and_y_axes = cc.executeIndivQuery(session, value)
    if x_and_y_axes == 'bad_key':
        # default if a bad key is supplied by the user for the query
        X = ["2020-02-05 22:45:38",
        "2020-02-05 22:45:39",
        "2020-02-05 22:45:40",
        "2020-02-05 22:45:41",
        "2020-02-05 22:45:42"]
        Y = [0,0,0,0,0]

    else:
        # time
        X = x_and_y_axes[0]
        # energy
        Y = x_and_y_axes[1]

    data = go.Scatter(
          x = X,
          y = Y,
          name = 'Scatter',
          mode = 'lines+markers'
    )
    # plotly doesn't usually update the axis range, only the points within the graph
    # that's why layout includes max and min range
    return {'data':[data], 'layout': go.Layout(xaxis = dict(range=[min(X), max(X)], title='Time'),
                                              yaxis = dict(range=[min(Y), max(Y)], title='Energy (kWh/s)'))}

# bar graph for top 10
@app.callback(Output('top-ten-graph', 'figure'),
              [Input('interval-component', 'n_intervals')])
def update_bar_graph_live(n):

    # columns = ['Geohash','Cumulative Energy']
    df = rc.queryForTopTenTable(r)

    X = df['Geohash'].tolist()
    Y = df['Cumulative Energy'].tolist()

    data = go.Bar(
        x = df['Geohash'].tolist(),
        y = df['Cumulative Energy'].tolist(),
        name = 'Bar'
    )


    return {'data':[data], 'layout': go.Layout( title="Top 10 Energy Users",
                                                xaxis=dict(
                                                    title='Geohash'),
                                                yaxis=dict(
                                                    range=[min(Y), max(Y)],
                                                    title='Energy (kWh)'))}

# table for latest outages on the grid
@app.callback(Output('outage-table', 'data'),
              [Input('interval-component', 'n_intervals')])
def update_table_live(n):

    # columns = ['Geohash','Timestamp']
    outageKey = "outageKey"
    df = rc.queryForAnomalyTables(r, outageKey)


    return df.to_dict('records')

# table for latest potential energy theft
@app.callback(Output('theft-table', 'data'),
              [Input('interval-component', 'n_intervals')])
def update_table_live(n):

    # columns = ['Geohash','Timestamp']
    outageKey = "theftKey"
    df = rc.queryForAnomalyTables(r, outageKey)

    return df.to_dict('records')

# geographic map that updates with data from cassandra
@app.callback(Output('choropleth-graph', 'figure'),
                [Input('interval-component-two', 'n_intervals')])
def update_map_graph(n):
    # get initial timestamp
    timestamp = cc.mostRecentTimestamp(session)


    # 'geohash','energy','GPS','lat','lon'
    df = cc.executeMapQuery(session, timestamp)



    fig = px.scatter_mapbox(df, lat="lat", lon="lon", hover_name='geohash',
                    hover_data=['energy'],
                    range_color=(0.00001, 0.002), center={"lat": 51.46006, "lon": -0.064767},
                    zoom=14)
    fig.update_layout(mapbox_style="open-street-map")
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    return fig





if __name__ == '__main__':

    # server.run(host="0.0.0.0", port=80)
    app.run_server(host="0.0.0.0", port=80)
