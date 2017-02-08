# Global definition
# Used for lookup operations during the flow stage

connectorDest = connectorDest.getConnection()
tabdCity = lib.table.SQLTable(
    connector=connectorDest
    , name='dCity'
    , column_names=['CityID', 'CityIDDB', 'PostalCode', 'CityName', 'Region', 'Country']
    , commit_count=1
    , pkey='CityID')
