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

type seatRequest: void{
	.seat[1,*]: void{
		.number: int
		.flightID: string
		.server: string
	}
}

type lockRequest: void{
	.seat[1,*]: void{
		.number: int
		.flightID: string
	}
	.transInfo: transInfo
}

interface FlightBookingInterface{
	OneWay: 
		requestLockIn(lockRequest), 			
	RequestResponse:
		canCommit(string)(bool),
		doCommit(string)(bool),
		abort(string)(void)
}

interface Coordinator{
	//OneWay: 
	RequestResponse: 
		getDecision(string)(bool),
		doCommit(string)(bool),
		book(seatRequest)(string)
}