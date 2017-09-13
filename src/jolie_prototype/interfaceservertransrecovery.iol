type tidattloc: void{
	.tid: string
	.att: int
	.coordloc: string
	.db: string
}

interface TransRecovery { 
	OneWay: recovery( string ) ,
                tryGetDecision( tidattloc ),
                coordinatorRecovery( string )
}

