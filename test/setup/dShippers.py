# Global definition
# Used for lookup operations during the flow stage

connectorDest = connectorDest.getConnection()
tabdShippers = lib.table.SQLTable(
    connector=connectorDest
    , name='dShippers'
    , column_names=['ShipperID', 'ShipperIDDB', 'CompanyName', 'Phone']
    , commit_count=1
    , pkey='ShipperID')
