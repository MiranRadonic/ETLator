connectorDest = connectorDest.getConnection()
tabdShippers = lib.table.SQLTable(
    connector=connectorDest
    , name='dShippers'
    , column_names=['ShipperID', 'ShipperIDDB', 'CompanyName', 'Phone']
    , commit_count=1
    , pkey='ShipperID'
    , default_args=default_args)


connectorSrc = connectorSrc.getConnection()
tabShippers = lib.table.SQLTable(
    connector=connectorSrc
    , name='Shippers'
    , column_names=['ShipperID', 'CompanyName', 'Phone', 'AuditID', 'AuditAction', 'AuditDate', 'AuditUser', 'AuditApp']
    , commit_count=1
    , pkey='ShipperID'
    , default_args=default_args)

for row in tabShippers:
    row['ShipperIDDB'] = row['ShipperID']
    if row['AuditAction'] == 'I':
        tabdShippers.insert(row)
    elif row['AuditAction'] == 'U':
        tabdShippers.update(row)
