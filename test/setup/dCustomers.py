# Global definition
# Used for lookup operations during the flow stage

connectorDest = connectorDest.getConnection()
tabdCustomers = lib.table.SQLTable(
    connector=connectorDest
    , name='dCustomers'
    , column_names=['CustomerID', 'CustomerIDDB', 'CompanyName', 'ContactName', 'ContactTitle', 'Address', 'CityID', 'CityIDDB', 'CityPostalCode', 'CityName', 'CityRegion', 'CityCountry', 'Phone', 'Fax', 'DateValidFrom', 'DateValidTo']
    , commit_count=1
    , pkey='CustomerID')
