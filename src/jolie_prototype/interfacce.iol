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
			.receiptForUndo?: string
		}
	}
}

type lockRequest: void{
	.seat[1,*]: void
	{
		.number: int
		.flightID: string
		.receiptForUndo?: string
	}
	.transInfo: transInfo
	.cid: string
	.receiptHash: string
}

type seatResp: void{
	.seat[0,*]: int 
	{
		.flight: string
	}
}

interface FlightBookingInterface{
	OneWay: 
		requestLockIn(lockRequest), 			
	RequestResponse:
		canCommit(transRequest)(bool),
		doCommit(transRequest)(bool),
		abort(transRequest)(void),
		getAvailableSeats(string)(seatResp)
}

interface Coordinator{
	//OneWay: 
	RequestResponse: 
		getDecision(string)(bool),
		book(seatRequest)(bookResponse),
}