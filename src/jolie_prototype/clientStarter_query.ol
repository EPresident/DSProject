include "interfacce.iol"
include "console.iol"
include "time.iol"
include "string_utils.iol"
include "database.iol"

constants
{
	dbname = "dbClient",
	serviceLocation = "socket://localhost:7999"
}

outputPort ClientService
{
	Location: serviceLocation
	Protocol: sodep
	Interfaces: InternalClientInterface
}

execution { single }

define showDB
{
	with ( connectionInfo ) 
	{
		.username = "sa";
		.password = "";
		.host = "";
		.database = "file:"+dbname;
		.driver = "sqlite"
	};
	connect@Database( connectionInfo )( void );
	println@Console("\n\t\t---RECEIPT---")();
	qr = "SELECT * FROM receipt";
	query@Database(qr)(qres);
	valueToPrettyString@StringUtils(qres)(str);
	println@Console(str)()
}

main 
{
	println@Console("Sending query...")();
	request.server = "socket://localhost:8001";
	
	query@ClientService(request)(response);
	valueToPrettyString@StringUtils(response)(str);
	println@Console("Done.\n"+str)()

}