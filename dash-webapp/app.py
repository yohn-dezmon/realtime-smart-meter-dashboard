from cassandraConnector import CassandraConnector
import datetime
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import pandas as pd
import dash_table
from redisConnector import RedisConnector
import redis
import geohash2
from geoJSON import GeoJSON
import plotly.express as px


ALLOWED_TYPES = (
    "text"
)


app = dash.Dash(__name__,
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
    html.H1('Real Time Energy'),
    dcc.Tabs(id="tabs-example", value='tab-1-example', children=[
        dcc.Tab(label='Community Map', value='tab-1-example'),
        dcc.Tab(label='Outages and Theft', value='tab-2-example'),
        dcc.Tab(label='Household Energy + Top Ten', value='tab-3-example')
    ]),
    html.Div(id='tabs-content-example')
])

# this callback creates the three tabs where the different graphs and tables are displayed
@app.callback(Output('tabs-content-example', 'children'),
              [Input('tabs-example', 'value')])
def render_content(tab):
    if tab == 'tab-1-example':
        return html.Div([
            html.Div( [
                html.H2('Community Energy Usage'),
                dcc.Graph(id='choropleth-graph'),
                dcc.Interval(
                    id='interval-component-two',
                    interval=5*1000, # in milliseconds
                    n_intervals=0
                )
            ])

        ])
    elif tab == 'tab-2-example':
        return html.Div([
        html.Div([
            html.H2('Latest Outages'),
            dcc.Interval(
            # this runs the method to obtain the data for the graph once every second
                id='interval-component',
                interval=1*1000, # in milliseconds
                n_intervals=0
            ),
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
    elif tab == 'tab-3-example':
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

                    ])
                        ])


# Multiple components can update everytime interval gets fired.
# this handles user input and individual time series graph
@app.callback(Output('live-update-graph', 'figure'),
              [Input('interval-component', 'n_intervals'),
              Input('input', 'value')])
def update_graph_live(n, value):
    """This dash callback method updates the individual user energy usage line graph"""


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
        # time (this is a pandas dataframe)
        X = x_and_y_axes[0]
        # energy (this is a pandas dataframe)
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

@app.callback(Output('top-ten-graph', 'figure'),
              [Input('interval-component', 'n_intervals')])
def update_bar_graph_live(n):
    """This dash callback method creates the bar graph for the Top 10 Energy users."""

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


@app.callback(Output('outage-table', 'data'),
              [Input('interval-component', 'n_intervals')])
def update_table_live(n):
    """This Dash callback method creates the table for the latest outages on the grid."""

    # columns = ['Geohash','Timestamp']
    outageKey = "outageKey"
    df = rc.queryForAnomalyTables(r, outageKey)


    return df.to_dict('records')


@app.callback(Output('theft-table', 'data'),
              [Input('interval-component', 'n_intervals')])
def update_table_live(n):
    """This Dash callback method creates the table for the latest potential energy theft on the grid."""

    # columns = ['Geohash','Timestamp']
    outageKey = "theftKey"
    df = rc.queryForAnomalyTables(r, outageKey)

    return df.to_dict('records')


@app.callback(Output('choropleth-graph', 'figure'),
                [Input('interval-component-two', 'n_intervals')])
def update_map_graph(n):
    """Dash callback method that creates a geographic map that updates with data from Cassandra."""
    # get initial timestamp
    timestamp = cc.mostRecentTimestamp(session)


    # 'geohash','energy','GPS','lat','lon'
    df = cc.executeMapQuery(session, timestamp)



    fig = px.scatter_mapbox(df, lat="lat", lon="lon", hover_name='geohash',
                    marker = dict(
                    color="green"
                    ),
                    hover_data=['energy'],
                    range_color=(0.00001, 0.002), center={"lat": 51.46006, "lon": -0.064767},
                    zoom=14)
    fig.update_layout(mapbox_style="open-street-map")
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    return fig





if __name__ == '__main__':

    app.run_server(host="0.0.0.0", port=80)
