include "interfacce.iol"
include "console.iol"
include "time.iol"

outputPort FlightBookingService {
  Location: "socket://localhost:8001"
  Protocol: sodep
  Interfaces: FlightBookingInterface, Coordinator
}

//devo conoscere tutte le compagnie e poter chiedere tutti i voli e i posti disponibili per ogni volo
//interface

define retry
{
        install (
		IOException => println@Console( "Server non disponibile retry"+(global.tent--) )();
                        sleep@Time(2000)();
                        retry
        );
	if (global.tent>0){
            request.lserv[0].server=FlightBookingService.location;
            request.lserv[0].seat[0].flightID="AZ0123";
            request.lserv[0].seat[0].number=69;
            request.lserv[0].seat[1].flightID="AZ0123";
            request.lserv[0].seat[1].number=70;
            request.lserv[1].server="socket://localhost:8000";
            request.lserv[1].seat[0].flightID="AZ4556";
            request.lserv[1].seat[0].number=44;
            println@Console(request)();
            book@FlightBookingService(request)(tid);
            println@Console("Ricevuto TID "+tid)()
        } else {
            println@Console( "Server non disponibile")()
        }
}

init
{
        global.tent=3;
        retry
}

main {
        println@Console("end")()
}