from flask import Flask, abort, redirect, request, render_template, jsonify
from cassandraConnector import CassandraConnector
# from wtforms import Form, TextField, TextAreaField, validators, StringField, SubmitField
# from IPython.display import HTML
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

# html.Div([])
app.layout = html.Div([
html.Div( [
    html.H4('Individual Electricity Usage'),
            html.Div(id='live-update-text'),
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
            html.H4('Top 10 Energy Users'),
            dcc.Graph(id='top-ten-graph')
            # dcc.Interval(
            # # this runs the method to obtain the data for the graph once every second
            #     id='interval-component',
            #     interval=1*1000, # in milliseconds
            #     n_intervals=0
            # )
        ]),

            # dash_table.DataTable(
            #         id='table',
            #         #{"name": i, "id": i} for i in df.columns])
            #         columns=[{'name': 'Geohash', 'id': 'Geohash'},
            #          {'name': 'Cumulative Energy', 'id': 'Cumulative Energy'}])]),

        html.Div([
            html.H4('Latest Outages'),
            dash_table.DataTable(
                id='outage-table',

                columns=[{'name': 'Geohash', 'id': 'Geohash'},
                 {'name': 'Timestamp', 'id': 'Timestamp'}])
                ]),
        html.Div([
            html.H4('Potential Energy Theft'),
            dash_table.DataTable(
                id='theft-table',

                columns=[{'name': 'Geohash', 'id': 'Geohash'},
                 {'name': 'Timestamp', 'id': 'Timestamp'}])
                ])

])

# Multiple components can update everytime interval gets fired.
# this handles user input and individual time series graph
@app.callback(Output('live-update-graph', 'figure'),
              [Input('interval-component', 'n_intervals'),
              Input('input', 'value')])
def update_graph_live(n, value):

    # I think I can use the 'value' parameter here to run the query!
    cc = CassandraConnector()
    session = cc.getSession()

    x_and_y_axes = cc.executeIndivQuery(session, value)
    if x_and_y_axes == 'bad_key':
        # this is a pandas dataframe
        # time
        X = ["2020-02-05 22:45:38",
        "2020-02-05 22:45:39",
        "2020-02-05 22:45:40",
        "2020-02-05 22:45:41",
        "2020-02-05 22:45:42"]
        Y = [0,0,0,0,0]

    else:
        # time
        X = x_and_y_axes[0]
        #
        # # energy
        Y = x_and_y_axes[1]

    data = go.Scatter(
          x = X,
          y = Y,
          name = 'Scatter',
          mode = 'lines+markers'
    )
    # plotly doesn't usually update the axis range, only the points within the graph
    # that's why layout
    return {'data':[data], 'layout': go.Layout(xaxis = dict(range=[min(X), max(X)], title='time'),
                                              yaxis = dict(range=[min(Y), max(Y)], title='energy'))}

# bar graph for top 10
@app.callback(Output('top-ten-graph', 'figure'),
              [Input('interval-component', 'n_intervals')])
def update_bar_graph_live(n):
    #query redis table
    rc = RedisConnector()
    r = redis.Redis()
    # columns = ['Geohash','Cumulative Energy']
    df = rc.queryForTopTenTable(r)

    data = go.Bar(
        x = df['Geohash'].tolist(),
        y = df['Cumulative Energy'].tolist(),
        name = 'Bar'
    )

    # df.to_dict()
    return {'data':[data], 'layout': go.Layout( title="Top 10",
                                                xaxis=dict(
                                                    title='Geohash'),
                                                yaxis=dict(
                                                    title='Energy (kWh)'))}




# @app.callback(Output('table', 'data'),
#               [Input('interval-component', 'n_intervals')])
# def update_table_live(n):
#     #query redis table
#     rc = RedisConnector()
#     r = redis.Redis()
#     # columns = ['Geohash','Cumulative Energy']
#     df = rc.queryForTopTenTable(r)
#
#     # df.to_dict()
#     return df.to_dict('records')

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


# host= '3.216.64.107:5000'
# app.run()
if __name__ == '__main__':
    # app.run_server()
    # app2.run_server()
    # server.run()
    server.run(host="0.0.0.0")
    # app.run_server(debug=True, host="0.0.0.0")
