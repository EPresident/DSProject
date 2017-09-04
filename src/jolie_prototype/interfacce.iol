type seat: void{
	.free: bool
	.tid?: string
}

type flight: void{
	.seat[1,*]: seat
}

type transInfo: void{
	.tid: string
	.coordLocation: string
}

type transRequest: void{
	.tid: string
	.cid: string
}

type seatRequest: void{
	.lserv[1,*]: void{	
		.server: string
		.seat[1,*]: void{
                    .number: int
                    .flightID: string
                    .cancel?:string
                }
	}
}

type lockRequest: void{
	.seat[1,*]: void{
		.number: int
		.flightID: string
		.cancel?:string
	}
	.cancel :string
	.transInfo: transInfo
	.cid: string
}

type bookResponse: void{
	.success: bool
	.receipt?: string
}

type flightResp: void{
	.flight[0,*]: string 
}

type seatResp: void{
	.seat[0,*]: int
}

type addR: void{
        .flightid : string
	.nseat : int
}

interface FlightBookingInterface{
	OneWay: 
		requestLockIn(lockRequest),
		addFlight(addR),
	RequestResponse:
		canCommit(transRequest)(bool),
		doCommit(transRequest)(bool),
		abort(transRequest)(void),
                getFlights(void)(flightResp),
		getAvarSeat(string)(seatResp)
}

interface Coordinator{
	//OneWay: 
	RequestResponse: 
		getDecision(string)(bool),
		book(seatRequest)(bookResponse)
}