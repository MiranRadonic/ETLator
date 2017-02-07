# Global definition
# Used for lookup operations during the flow stage

connectorDest = connectorDest.getConector()
tabdEmployees = lib.table.SQLTable(
    connector=connectorDest
    , name='dEmployees'
    , column_names=['EmployeeID', 'EmployeeIDDB', 'LastName', 'FirstName', 'FullName', 'Title', 'TitleOfCourtesy', 'BirthDate', 'HireDate', 'Address', 'CityID', 'CityIDDB', 'CityPostalCode', 'CityName', 'CityRegion', 'CityCountry', 'HomePhone', 'Extension', 'ReportsTo']
    , commit_count=1
    , pkey='EmployeeID')
