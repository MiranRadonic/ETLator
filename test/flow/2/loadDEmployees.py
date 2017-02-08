connectorDest = connectorDest.getConnection()
tabdEmployees = lib.table.SQLTable(
    connector=connectorDest
    , name='dEmployees'
    , column_names=['EmployeeID', 'EmployeeIDDB', 'LastName', 'FirstName', 'FullName', 'Title', 'TitleOfCourtesy', 'BirthDate', 'HireDate', 'Address', 'CityID', 'CityIDDB', 'CityPostalCode', 'CityName', 'CityRegion', 'CityCountry', 'HomePhone', 'Extension', 'ReportsTo']
    , commit_count=1
    , pkey='EmployeeID'
    , default_args=default_args)


connectorSrc = connectorSrc.getConnection()
tabEmployees = lib.table.SQLTable(
    connector=connectorSrc
    , name='Employees'
    , column_names=['EmployeeID', 'LastName', 'FirstName', 'Title', 'TitleOfCourtesy', 'BirthDate', 'HireDate', 'Address', 'CityID', 'HomePhone', 'Extension', 'Notes', 'ReportsTo', 'AuditID', 'AuditAction', 'AuditDate', 'AuditUser', 'AuditApp']
    , commit_count=1
    , pkey='CustomerID'
    , default_args=default_args)


for row in tabEmployees:
    row['CityIDDB'] = row['CityID']
    lkp = tabdCity.lookup(row, ['CityIDDB'])
    if lkp is not None:
        row['CityID'] = lkp['CityIDDB']
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

    row['EmployeeIDDB'] = row['EmployeeID']
    try:
        row['FullName'] = row['FirstName'] + ' ' + row['LastName']
    except:
        row['FullName'] = ''
    if row['AuditAction'] == 'I':
        tabdEmployees.insert(row)
    elif row['AuditAction'] == 'U':
        tabdEmployees.update(row)
