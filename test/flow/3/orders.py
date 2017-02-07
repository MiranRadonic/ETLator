connectorDest = connectorDest.getConector()
tabfOrders = lib.table.SQLTable(
    connector=connectorDest
    , name='fOrders'
    , column_names=['OrderID', 'OrderIDDB', 'PaymentMethod', 'Freight', 'ShipName', 'ShipAddress', 'CustomerID', 'EmployeeID', 'OrderDate', 'RequiredDate', 'ShippedDate', 'ShipVia', 'ShipCityID', 'DeliveryDays', 'LateDays']
    , commit_count=10
    , pkey='OrderID'
    , default_args=default_args
    , show_progress=True)


connectorSrc = connectorSrc.getConector()
tabOrders = lib.table.SQLTable(
    connector=connectorSrc
    , name='Orders'
    , column_names=['OrderID', 'PaymentMethod', 'Freight', 'ShipName', 'ShipAddress', 'CustomerID', 'EmployeeID', 'OrderDate', 'RequiredDate', 'ShippedDate', 'ShipVia', 'ShipCityID', 'AuditID', 'AuditAction', 'AuditDate', 'AuditUser', 'AuditApp']
    , id_insert=True
    , commit_count=1
    , pkey='OrderID'
    , default_args=default_args)


for row in tabOrders:
    #print row
    row['OrderIDDB'] = row['OrderID']

    row['CustomerIDDB'] = row['CustomerID']
    custLkp = tabdCustomers.lookup(row, ['CustomerIDDB'])
    if custLkp is not None:
        row['CustomerID'] = custLkp['CustomerID']
    else:
        row['CustomerID'] = 0

    row['EmployeeIDDB'] = row['EmployeeID']
    empLkp = tabdEmployees.lookup(row, ['EmployeeIDDB'])
    if empLkp is not None:
        row['EmployeeID'] = empLkp['EmployeeID']
    else:
        row['EmployeeID'] = 0

    row['CityIDDB'] = row['ShipCityID']
    ctyLkp = tabdCity.lookup(row, ['CityIDDB'])
    if ctyLkp is not None:
        row['ShipCityID'] = ctyLkp['CityID']
    else:
        row['ShipCityID'] = 0

    row['ShipperIDDB'] = row['ShipVia']
    shpLkp = tabdShippers.lookup(row, ['ShipperIDDB'])
    if shpLkp is not None:
        row['ShipVia'] = shpLkp['ShipperID']
        if row['ShipVia'] is None:
            row['ShipVia'] = 0
    else:
        row['ShipVia'] = 0
    # Dates

    # Order Date
    od = None
    if row['OrderDate'] is None:
        row['OrderDate'] = 1
    else:
        od = row['OrderDate']
        row['date'] = row['OrderDate']
        odLkp = tabdDate.lookup(row, ['date'])
        row['OrderDate'] = odLkp['dateID']
        if row['OrderDate'] is None:
            row['OrderDate'] = 1
    # Required Date
    rd = None
    if row['RequiredDate'] is None:
        row['RequiredDate'] = 1
    else:
        rd = row['RequiredDate']
        row['date'] = row['RequiredDate']
        odLkp = tabdDate.lookup(row, ['date'])
        row['RequiredDate'] = odLkp['dateID']
        if row['RequiredDate'] is None:
            row['RequiredDate'] = 1
    # Shipped Date
    sd = None
    if row['ShippedDate'] is None:
        row['ShippedDate'] = 1
    else:
        sd = row['ShippedDate']
        row['date'] = row['ShippedDate']
        odLkp = tabdDate.lookup(row, ['date'])
        row['ShippedDate'] = odLkp['dateID']
        if row['ShippedDate'] is None:
            row['ShippedDate'] = 1

    row['DeliveryDays'] = None
    row['LateDays'] = None
    if od is not None and sd is not None:
        row['DeliveryDays'] = int((sd - od).days)
    if rd is not None and sd is not None:
        row['LateDays'] = int((sd - rd).days)

    if row['ShipCityID'] is None:
        row['ShipCityID'] = 0
    if row['CustomerID'] is None:
        row['CustomerID'] = 0
    if row['EmployeeID'] is None:
        row['EmployeeID'] = 0
    if row['ShipVia'] is None:
        row['ShipVia'] = 0
    if row['PaymentMethod'] is None:
        row['PaymentMethod'] = 'Unknown'
    if row['ShipName'] is None:
        row['ShipName'] = 'Unknown'
    if row['ShipAddress'] is None:
        row['ShipAddress'] = 'Unknown'

    if row['AuditAction'] == 'I':
        tabfOrders.insert(row)
    elif row['AuditAction'] == 'U':
        tabfOrders.update(row)
