--DROP DATABASE LINK "ECUADOR";
CREATE DATABASE LINK "ECUADOR"
   CONNECT TO "AGRO" IDENTIFIED BY a2g0r1o8america
   USING '(DESCRIPTION =
    (ADDRESS_LIST =
      (ADDRESS = (PROTOCOL = TCP)(HOST = 69.87.220.61)(PORT = 1521))
    )
    (CONNECT_DATA =
      (SERVICE_NAME = XE)
    )
  )';

--DROP DATABASE LINK "AGRICULTURA";
CREATE DATABASE LINK "AGRICULTURA"
   CONNECT TO "AGRICULTURA" IDENTIFIED BY agricultura
   USING '(DESCRIPTION =
    (ADDRESS_LIST =
      (ADDRESS = (PROTOCOL = TCP)(HOST = jdedb.cjfqrw8mt8l5.us-east-1.rds.amazonaws.com)(PORT = 5432))
    )
    (CONNECT_DATA =
      (SERVICE_NAME = db900)
    )
  )';

CREATE DATABASE LINK "VEGAS"
   CONNECT TO "BANASA" IDENTIFIED BY plaomgt08
   USING '(DESCRIPTION =
    (ADDRESS_LIST =
      (ADDRESS = (PROTOCOL = TCP)(HOST = 172.17.3.196)(PORT = 1521))
    )
    (CONNECT_DATA =
      (SERVICE_NAME = vegas)
    )
  )';
  select * from trabajos@pana;
  
  CREATE DATABASE LINK "SIERRA"
   CONNECT TO "BANASA" IDENTIFIED BY plabanasals09
   USING '(DESCRIPTION =    
(ADDRESS_LIST =      
(ADDRESS = (PROTOCOL = TCP)(HOST = 172.17.3.196)(PORT = 1521))    
)    
(CONNECT_DATA =      
(SERVICE_NAME = sierra)    
)  
)';

CREATE DATABASE LINK "PANA"
   CONNECT TO "PLANILLA" IDENTIFIED BY planilla
   USING '(DESCRIPTION =
    (ADDRESS_LIST =
      (ADDRESS = (PROTOCOL = TCP)(HOST = 172.17.3.196)(PORT = 1521))
    )
    (CONNECT_DATA =
      (SERVICE_NAME = panajachel)
    )
  )';
  
  CREATE DATABASE LINK "FRONTERA"
   CONNECT TO "BANASA" IDENTIFIED BY plabanasagt08
   USING '(DESCRIPTION =
    (ADDRESS_LIST =
      (ADDRESS = (PROTOCOL = TCP)(HOST = 172.17.3.196)(PORT = 1521))
    )
    (CONNECT_DATA =
      (SERVICE_NAME = frontera)
    )
  )';
  
  CREATE DATABASE LINK "AGROSTG"
   CONNECT TO "AGROSTG" IDENTIFIED BY manager1
   USING '(DESCRIPTION =
    (ADDRESS_LIST =
      (ADDRESS = (PROTOCOL = TCP)(HOST = 172.17.3.164)(PORT = 5432))
    )
    (CONNECT_DATA =
      (SERVICE_NAME = obiagro)
    )
  )';
  select * from stg_locacion@agrostg;