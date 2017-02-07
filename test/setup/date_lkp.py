# Global definition
# Used for lookup operations during the flow stage

connectorDest = connectorDest.getConector()
tabdDate = lib.table.SQLTable(
    connector=connectorDest
    , name='dDate'
    , column_names=['dateID', 'date', 'type', 'dayMonthYear', 'day', 'month', 'year', 'quarter', 'nrDayWeek', 'nameDayWeek', 'nameMonth', 'isWorkDay', 'isLastDayMonth, season', 'event', 'holiday', 'nameHoliday']
    , commit_count=1
    , pkey='dateID')


connectorDest = connectorDest.getConector()
tabdTimeDay = lib.table.SQLTable(
    connector=connectorDest
    , name='dTimeDay'
    , column_names=['timeDayID', 'type', 'secondsMidnight', 'minutesMidnight', 'time', 'second', 'minute', 'hour', 'hourMinSec', 'period', 'officeHours']
    , commit_count=1
    , pkey='timeDayID')
