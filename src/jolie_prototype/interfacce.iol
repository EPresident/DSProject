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
	.clientLocation: string
	.timeout?: int
}

type lockRequest: void{
	.seat[1,*]: void
	{
		.number: int
		.flightID: string
		.receiptForUndo?: string
	}
	.receiptHash :string
	.transInfo: transInfo
	.cid: string
}

type seatResp: void{
	.seat[0,*]: int 
	{
		.flight: string
	}
}

type bookResponse: void{
	.success: bool
	.receipt?: string
}

type queryRequest: void{
	.server: string
	.receiptHash?: string
}

interface ClientInterface{
	RequestResponse: 
		canCommit(string)(bool)
}

interface InternalClientInterface{
	OneWay:
		book(seatRequest),
	RequestResponse:
		query(queryRequest)(seatResp)
}

interface FlightBookingInterface{
	OneWay: 
		requestLockIn(lockRequest),
	RequestResponse:
		canCommit(transRequest)(bool),
		doCommit(transRequest)(bool) throws IOException,
		abort(transRequest)(void) throws IOException,
		getAvailableSeats(void)(seatResp),
		getReservedSeats(string)(seatResp)
}

interface Coordinator{
	//OneWay: 
	RequestResponse: 
	getDecision(string)(bool) throws NotDecidedException,
	book(seatRequest)(bookResponse) throws InterruptedException
}