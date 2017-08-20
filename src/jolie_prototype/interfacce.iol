type seat: void{
	.free: bool
	.tid?: tid
}

type flight: void{
	.seat[1,*]: seat
}

type tid: void{
	.issuer: string
	.id: int
	.location: string
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
	.tid: tid
}

interface FlightBookingInterface{
	OneWay: 
		requestLockIn(lockRequest), 			
	RequestResponse:
		canCommit(tid)(bool),
		doCommit(tid)(bool),
		abort(tid)(void)
}

interface Coordinator{
	//OneWay: 
	RequestResponse: 
		getDecision(tid)(bool),
		doCommit(tid)(bool),
		book(seatRequest)(tid)
}