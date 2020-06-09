# https://dash.plot.ly/live-updates
import plotly
import plotly.graph_objects as go
import plotly.figure_factory as ff
import happybase
CONNECTION = happybase.Connection('localhost', 9090)
# TABLE_NAME = "covid19_us_counties"
TABLE_NAME = "covid_us_counties"
table = CONNECTION.table(TABLE_NAME)

def get_sum_by_county():
    tmp_data = {}
    for k, data in table.scan():
        fips = data[b"covid_data:fips"].decode("utf-8")
        cases = data[b"covid_data:cases"].decode("utf-8")
        if fips in tmp_data: 
            tmp_data[fips] = int(cases) + tmp_data.get(fips)
        else:
            tmp_data[fips] = int(cases)
    
    return tmp_data

def update_graph_live(n):
    data = get_sum_by_county()
    fips = list(data.keys())
    cases = list(data.values())

    fig = ff.create_choropleth(fips=fips, values=cases)
    fig.layout.template = None
    fig.show()

update_graph_live(100)
if __name__ == '__main__':
    app.run_server(debug=True)