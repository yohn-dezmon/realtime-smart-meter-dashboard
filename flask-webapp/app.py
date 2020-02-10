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
    'text': '#7FDBFF'
}


app.layout = html.Div([
# html.Div( [
#     html.H4('Individual Electricity Usage'),
#             html.Div(id='live-update-text'),
#             dcc.Input(id='input', value='', type='text'),
#             dcc.Graph(id='live-update-graph'),



#         ]),
#         html.Div([
#             html.H4('Top 10 Energy Users'),
#             dcc.Graph(id='top-ten-graph')
#         ]),
        html.Div([
        dcc.Interval(
        # this runs the method to obtain the data for the graph once every second
            id='interval-component',
            interval=1*1000, # in milliseconds
            n_intervals=0
        ),

            html.H4('Latest Outages'),
            dash_table.DataTable(
                id='outage-table',
                style_cell={
                'textAlign': 'center',
                'width' : '600px',
                'backgroundColor' : colors['background'],
                'color': colors['text']
                },
                columns=[{'name': 'Geohash', 'id': 'Geohash'},
                 {'name': 'Timestamp', 'id': 'Timestamp'}])
                ]),
        html.Div([
            html.H4('Potential Energy Theft'),
            dash_table.DataTable(
                id='theft-table',

                columns=[{'name': 'Geohash', 'id': 'Geohash'},
                 {'name': 'Timestamp', 'id': 'Timestamp'}])
                ]),
    html.Div( [
        html.H4('Community Energy Usage'),
        dcc.Graph(id='choropleth-graph'),
        dcc.Interval(
        # this runs the method to obtain the data for the graph once every second
            id='interval-component-two',
            interval=25*1000, # in milliseconds
            n_intervals=0
        )
    ])


])

# Multiple components can update everytime interval gets fired.
# this handles user input and individual time series graph
# @app.callback(Output('live-update-graph', 'figure'),
#               [Input('interval-component', 'n_intervals'),
#               Input('input', 'value')])
# def update_graph_live(n, value):
#
#     # I think I can use the 'value' parameter here to run the query!
#     cc = CassandraConnector()
#     session = cc.getSession()
#
#     x_and_y_axes = cc.executeIndivQuery(session, value)
#     if x_and_y_axes == 'bad_key':
#         # default if a bad key is supplied by the user for the query
#         X = ["2020-02-05 22:45:38",
#         "2020-02-05 22:45:39",
#         "2020-02-05 22:45:40",
#         "2020-02-05 22:45:41",
#         "2020-02-05 22:45:42"]
#         Y = [0,0,0,0,0]
#
#     else:
#         # time
#         X = x_and_y_axes[0]
#         #
#         # # energy
#         Y = x_and_y_axes[1]
#
#     data = go.Scatter(
#           x = X,
#           y = Y,
#           name = 'Scatter',
#           mode = 'lines+markers'
#     )
#     # plotly doesn't usually update the axis range, only the points within the graph
#     # that's why layout includes max and min range
#     return {'data':[data], 'layout': go.Layout(xaxis = dict(range=[min(X), max(X)], title='time'),
#                                               yaxis = dict(range=[min(Y), max(Y)], title='energy'))}

# bar graph for top 10
# @app.callback(Output('top-ten-graph', 'figure'),
#               [Input('interval-component', 'n_intervals')])
# def update_bar_graph_live(n):
#     #query redis table
#     rc = RedisConnector()
#     r = redis.Redis()
#     # columns = ['Geohash','Cumulative Energy']
#     df = rc.queryForTopTenTable(r)
#
#     X = df['Geohash'].tolist()
#     Y = df['Cumulative Energy'].tolist()
#
#     data = go.Bar(
#         x = df['Geohash'].tolist(),
#         y = df['Cumulative Energy'].tolist(),
#         name = 'Bar'
#     )
#
#
#     return {'data':[data], 'layout': go.Layout( title="Top 10",
#                                                 xaxis=dict(
#                                                     title='Geohash'),
#                                                 yaxis=dict(
#                                                     range=[min(Y), max(Y)],
#                                                     title='Energy (kWh)'))}


@app.callback(Output('outage-table', 'data'),
              [Input('interval-component', 'n_intervals')])
def update_table_live(n):
    #query redis table
    rc = RedisConnector()
    r = redis.Redis()
    # columns = ['Geohash','Timestamp']
    outageKey = "outageKey"
    df = rc.queryForAnomalyTables(r, outageKey)


    return df.to_dict('records')

@app.callback(Output('theft-table', 'data'),
              [Input('interval-component', 'n_intervals')])
def update_table_live(n):
    #query redis table
    rc = RedisConnector()
    r = redis.Redis()
    # columns = ['Geohash','Timestamp']
    outageKey = "theftKey"
    df = rc.queryForAnomalyTables(r, outageKey)

    return df.to_dict('records')

@app.callback(Output('choropleth-graph', 'figure'),
                [Input('interval-component-two', 'n_intervals')])
def update_map_graph(n):

    cc = CassandraConnector()
    session = cc.getSession()
    geoJSON = GeoJSON()

    # amount of seconds to subtract when querying to account for delay
    # from ingestion to insertion into cassandra
    secs = 7

    now = datetime.datetime.now()
    nowNearestSecond = now - datetime.timedelta(microseconds=now.microsecond)
    nowMinusSeconds = now - datetime.timedelta(seconds=secs)
    timestamp = nowMinusSeconds.strftime("%Y-%m-%d %H:%M:%S")

    # 'geohash','energy'
    df = cc.executeMapQuery(session, timestamp)


    # create GeoJSON for plotly chloropleth
    geoJSONList = geoJSON.createGeoJSON(df)

    # locations='geohash'
    fig = px.choropleth_mapbox(df, geojson=geoJSONList, locations='GPS', color='energy',
                        color_continuous_scale="Viridis",
                        range_color=(0,12),
                        # scope='world',
                        mapbox_style="carto-positron",
                        # 51.452802, -0.075267
                           zoom=11, center = {"lat": 51.4528, "lon": -0.0752},
                         labels={'energy':'Energy (kWh/s)'}
                         )
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    return fig

    # data = go.Scatter(
    #       x = X,
    #       y = Y,
    #       name = 'Scatter',
    #       mode = 'lines+markers'
    # )
    # plotly doesn't usually update the axis range, only the points within the graph
    # that's why layout includes max and min range
    # return {'data':[data], 'layout': go.Layout(xaxis = dict(range=[min(X), max(X)], title='time'),
    #                                           yaxis = dict(range=[min(Y), max(Y)], title='energy'))}




@server.route('/', methods=['GET','POST'])
def index_get():
    """ This is the homepage of the website
    """

    #list_of_lists = cc.executeIndivQuery(session)
    list_of_lists = [[]]
    if request.method == "POST":
        try:
            geohash = request.form.get("geohash")
            # print(email_name + " " + name)
            return render_template("index.html",
            list_of_lists=list_of_lists,
            geohash=geohash)
        except Exception as e:
            print("geohash not found")
            print(e)

            return render_template("index.html",
            list_of_lists=list_of_lists)
    else:
        return render_template('index.html',
                                list_of_lists=list_of_lists)


@server.route('/query-example')
def query_example():
    # the args.get() returns None if key doesn't exist! :D
    language = request.args.get('language')

    return '''<h1>The language value is: {}</h1>'''.format(language)



if __name__ == '__main__':

    server.run(host="0.0.0.0", port=80)
    # app.run_server(host="0.0.0.0")
