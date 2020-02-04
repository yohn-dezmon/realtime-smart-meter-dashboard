from flask import Flask, abort, redirect, request, render_template
from cassandraConnector import CassandraConnector
# from IPython.display import HTML

app = Flask(__name__)


@app.route('/', methods=['GET'])
def index_get():
    """ This is the homepage of the website
    """
    variable1 = "variable!"
    cc = CassandraConnector()
    session = cc.getSession()
    list_of_lists = cc.executeIndivQuery(session)

    return render_template('index.html',
                            variable1=variable1,
                            list_of_lists=list_of_lists)


# host= '3.216.64.107:5000'
app.run()
