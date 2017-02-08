connectorDest = connectorDest.getConnection()
tabdCity = lib.table.SQLTable(
    connector=connectorDest
    , name='dCity'
    , column_names=['CityID', 'CityIDDB', 'PostalCode', 'CityName', 'Region', 'Country']
    , commit_count=1
    , pkey='CityID'
    , default_args=default_args)


connectorSrc = connectorSrc.getConnection()
tabCity = lib.table.SQLTable(
    connector=connectorSrc
    , name='City'
    , column_names=['CityID', 'PostalCode', 'CityName', 'Region', 'Country', 'AuditID', 'AuditAction', 'AuditDate', 'AuditUser', 'AuditApp']
    , commit_count=1
    , pkey='CityID'
    , default_args=default_args)

for row in tabCity:
    row['CityIDDB'] = row['CityID']
    if row['AuditAction'] == 'I':
        tabdCity.insert(row)
    elif row['AuditAction'] == 'U':
        tabdCity.update(row)
