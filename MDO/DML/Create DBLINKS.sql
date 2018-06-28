--With configured TNS Names on AGROSTG.
create database link evolution connect to "sa" identified by "EvoSA2016" using 'evolution';

CREATE DATABASE LINK "FRONTERA"
   CONNECT TO "BANASA" IDENTIFIED BY plabanasagt08
   USING '(DESCRIPTION =
    (ADDRESS_LIST =
      (ADDRESS = (PROTOCOL = TCP)(HOST = 192.168.3.236)(PORT = 1521))
    )
    (CONNECT_DATA =
      (SERVICE_NAME = frontera)
    )
  )';

CREATE DATABASE LINK "SIERRA"
   CONNECT TO "BANASA" IDENTIFIED BY plabanasals09
   USING '(DESCRIPTION =    
(ADDRESS_LIST =      
(ADDRESS = (PROTOCOL = TCP)(HOST = 192.168.3.236)(PORT = 1521))    
)    
(CONNECT_DATA =      
(SERVICE_NAME = sierra)    
)  
)';

CREATE DATABASE LINK "VEGAS"
   CONNECT TO "BANASA" IDENTIFIED BY plaomgt08
   USING '(DESCRIPTION =
    (ADDRESS_LIST =
      (ADDRESS = (PROTOCOL = TCP)(HOST = 192.168.3.236)(PORT = 1521))
    )
    (CONNECT_DATA =
      (SERVICE_NAME = vegas)
    )
  )';
  
CREATE DATABASE LINK "PANA"
   CONNECT TO "PLANILLA" IDENTIFIED BY planilla
   USING '(DESCRIPTION =
    (ADDRESS_LIST =
      (ADDRESS = (PROTOCOL = TCP)(HOST = 192.168.3.236)(PORT = 1521))
    )
    (CONNECT_DATA =
      (SERVICE_NAME = XE)
    )
  )';