# Deephaven application mode (-Ddeephaven.application.dir=/app.d): runs concert.py at startup and
# exports its tables as application fields (widgets addressable by name, e.g. /iframe/widget/?name=quotes_latest).
type=script
scriptType=python
enabled=true
id=io.concert.analytics
name=Concert analytics
file_0=concert.py
