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
            request.seat[0].number=69;
            request.seat[0].flightID="AZ0123";
            request.seat[0].server=FlightBookingService.location;
            request.seat[1].number=44;
            request.seat[1].flightID="AZ4556";
            request.seat[1].server="socket://localhost:8000";
            println@Console(request)();
            book@FlightBookingService(request)(tid);
            println@Console("Ricevuto TID "+tid.issuer+tid.id)()
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