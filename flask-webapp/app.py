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

# pip install pyorbital
# from pyorbital.orbital import Orbital
# satellite = Orbital('TERRA')

X = deque(maxlen=20)
Y = deque(maxlen=20)
X.append(1)
Y.append(1)

server = Flask(__name__)


app = dash.Dash(__name__,
                server=server,
                routes_pathname_prefix='/dash/'
                )
app.layout = html.Div([
html.H4('TERRA Satellite Live Feed'),
        html.Div(id='live-update-text'),
        dcc.Graph(id='live-update-graph'),
        dcc.Interval(
        # this runs the method to obtain the data for the graph once every second
            id='interval-component',
            interval=1*1000, # in milliseconds
            n_intervals=0
        )
])


# Multiple components can update everytime interval gets fired.
@app.callback(Output('live-update-graph', 'figure'),
              [Input('interval-component', 'n_intervals')])
def update_graph_live(n):
    X.append(X[-1]+1)
    Y.append(Y[-1]+(Y[-1]*random.uniform(-0.1,0.1)))

    data = go.Scatter(
          x = list(X),
          y = list(Y),
          name = 'Scatter',
          mode = 'lines+markers'
    )
    # plotly doesn't usually update the axis range, only the points within the graph
    # that's why layout
    return {'data':[data], 'layout': go.Layout(xaxis = dict(range=[min(X), max(X)]),
                                              yaxis = dict(range=[min(Y), max(Y)]))}


@server.route('/', methods=['GET','POST'])
def index_get():
    """ This is the homepage of the website
    """


    cc = CassandraConnector()
    session = cc.getSession()



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
    app.run_server()
