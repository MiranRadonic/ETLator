connectorDest = connectorDest.getConnection()
tabdCustomers = lib.table.SCDTable(
    connector=connectorDest
    , name='dCustomers'
    , column_names=['CustomerID', 'CustomerIDDB', 'CompanyName', 'ContactName', 'ContactTitle', 'Address', 'CityID', 'CityIDDB', 'CityPostalCode', 'CityName', 'CityRegion', 'CityCountry', 'Phone', 'Fax', 'DateValidFrom', 'DateValidTo']
    , scd_lookup_columns=['CustomerIDDB']
    , commit_count=1
    , date_from_cloumn='DateValidFrom'
    , date_to_column='DateValidTo'
    , pkey='CustomerID'
    , default_args=default_args)


connectorSrc = connectorSrc.getConnection()
tabCustomers = lib.table.SQLTable(
    connector=connectorSrc
    , name='Customers'
    , column_names=['CustomerID', 'CompanyName', 'ContactName', 'ContactTitle', 'Address', 'CityID', 'Phone', 'Fax', 'AuditID', 'AuditAction', 'AuditDate', 'AuditUser', 'AuditApp']
    , commit_count=1
    , pkey='CustomerID'
    , default_args=default_args)


for row in tabCustomers:
    row['CityIDDB'] = row['CityID']
    lkp = tabdCity.lookup(row, ['CityIDDB'])
    if lkp is not None:
        row['CityID'] = lkp['CityID']
    else:
        row['CityID'] = 0
        lkp = tabdCity.lookup(row, ['CityID'])
    try:
        row['CityPostalCode'] = lkp['PostalCode']
        row['CityName'] = lkp['CityName']
        row['CityRegion'] = lkp['Region']
        row['CityCountry'] = lkp['Country']
    except:
        row['CityPostalCode'] = None
        row['CityName'] = None
        row['CityRegion'] = None
        row['CityCountry'] = None

    row['CustomerIDDB'] = row['CustomerID']
    row['DateValidFrom'] = datetime.now()
    row['DateValidTo'] = None

    if row['AuditAction'] == 'I':
        tabdCustomers.insert(row)

    elif row['AuditAction'] == 'U':
        tabdCustomers.date_from_value=row['AuditDate']
        tabdCustomers.scd_update(row)
