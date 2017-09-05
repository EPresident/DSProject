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

/*outputPort ClientService
{
	Interfaces: InternalClientInterface
}

embedded {
	Jolie: "client.ol" in ClientService
}*/
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
	println@Console("Invio richiesta...")();
	request.lserv[0].server="socket://localhost:8001";
	request.lserv[0].seat[0].flightID="AZ0123";
	request.lserv[0].seat[0].number=69;
	request.lserv[0].seat[1].flightID="AZ0123";
	request.lserv[0].seat[1].number=70;
	request.lserv[1].server="socket://localhost:8000";
	request.lserv[1].seat[0].flightID="AZ4556";
	request.lserv[1].seat[0].number=44;
	
	book@ClientService(request);
	println@Console("Fatto.")();
	showDB

}