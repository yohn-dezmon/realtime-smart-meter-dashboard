## Dash Webapp

Within my webserver EC2 instance I set up a virtual environment using:

```
$ pip3 install virtualenv
$ touch ~/.venvs
$ cd ~/.venvs
$ virutalenv smartmeter
```

To instantiate the virtualenv:
```
$ . ~/.venvs/smartmeter/bin/activate
```

Now that the virtual environment is set up, you must install the below dependencies
using pip3.

Python Dependencies:
```
cassandra-driver==3.21.0
Click==7.0
dash==1.9.0
dash-core-components==1.8.0
dash-html-components==1.0.2
dash-renderer==1.2.4
dash-table==4.6.0
docutils==0.16
Flask==1.1.1
Flask-Compress==1.4.0
future==0.18.2
geohash2==1.1
geojson==2.5.0
geomet==0.1.2
itsdangerous==1.1.0
Jinja2==2.11.1
MarkupSafe==1.1.1
numpy==1.18.1
pandas==0.24.2
plotly==4.5.0
python-dateutil==2.8.1
pytz==2019.3
redis==3.4.1
retrying==1.3.3
six==1.14.0
Werkzeug==0.16.1
```
Now that the correct dependencies have been installed, change your configurations for Cassandra in the config.json file such that the cassandra, node 1 key points to the public IP of your instances where you have Cassandra installed.

To run the application, cd into dash-webapp folder on your EC2 instance and run the
following in the command line:
```
sudo ~/.venvs/smartmeter/bin/python3 app.py
```

You can now access the dash-application by going to:
```
http://0.0.0.0:80/
```
