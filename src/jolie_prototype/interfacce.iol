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
                    }
	}
}

type lockRequest: void{
	.seat[1,*]: void{
		.number: int
		.flightID: string
	}
	.transInfo: transInfo
	.cid: string
}

interface FlightBookingInterface{
	OneWay: 
		requestLockIn(lockRequest), 			
	RequestResponse:
		canCommit(transRequest)(bool),
		doCommit(transRequest)(bool),
		abort(transRequest)(void)
}

interface Coordinator{
	//OneWay: 
	RequestResponse: 
		getDecision(string)(bool),
		book(seatRequest)(string)
}