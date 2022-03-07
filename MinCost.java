package minCost;

import ilog.concert.IloException;
import ilog.concert.IloNumVar;
import ilog.cplex.IloCplex;

//分别计算map和reduce两阶段的最小化成本的任务放置策略
public class MinCost {
	public int n;
	public double[] comCost;
	public double[][] bdCost;
	public double[] input;
	
	public double[] bandWidthUp;
	public double[] bandWidthDown;
	
	public double interSum;
	
	public int[]  slot;
	
	//每1GB数据需要多少slot
	public  double  u;
	
	//中间数据和输入数据的比值
	public double   mapToReduce;
	
	//每一个map花费的时间
	public double   tmap;
	
	//每一个reduce花费的时间
	public double   tred;
	
	//完成单个map任务和reduce任务时间比值
	public double   q;
	
	//构造函数
	public MinCost(int n, double[] comCost, double[][] bdCost, double[] input,
			double[] bandWidthUp, double[] bandWidthDown, double interSum,
			int[] slot, double u, double mapToReduce, double q, double tmap, double tred) {
		super();
		this.n = n;
		this.comCost = comCost;
		this.bdCost = bdCost;
		this.input = input;
		this.bandWidthUp = bandWidthUp;
		this.bandWidthDown = bandWidthDown;
		this.interSum = interSum;
		this.slot = slot;
		this.u = u;
		this.mapToReduce = mapToReduce;
		this.q = q;
		this.tmap=tmap;
		this.tred=tred;
	}
	
	public double getTmap() {
		return tmap;
	}

	public void setTmap(double tmap) {
		this.tmap = tmap;
	}

	public double getTred() {
		return tred;
	}

	public void setTred(double tred) {
		this.tred = tred;
	}

	//从超类继承的构造函数
	public MinCost() {
		super();
		// TODO Auto-generated constructor stub
	}
	
	//设置和获得成员变量值
	public int getN() {
		return n;
	}

	public void setN(int n) {
		this.n = n;
	}

	public double[] getComCost() {
		return comCost;
	}

	public void setComCost(double[] comCost) {
		this.comCost = comCost;
	}

	public double[][] getBdCost() {
		return bdCost;
	}

	public void setBdCost(double[][] bdCost) {
		this.bdCost = bdCost;
	}

	public double[] getInput() {
		return input;
	}

	public void setInput(double[] input) {
		this.input = input;
	}

	public double[] getBandWidthUp() {
		return bandWidthUp;
	}

	public void setBandWidthUp(double[] bandWidthUp) {
		this.bandWidthUp = bandWidthUp;
	}

	public double[] getBandWidthDown() {
		return bandWidthDown;
	}

	public void setBandWidthDown(double[] bandWidthDown) {
		this.bandWidthDown = bandWidthDown;
	}

	public double getInterSum() {
		return interSum;
	}

	public void setInterSum(double interSum) {
		this.interSum = interSum;
	}

	public int[] getSlot() {
		return slot;
	}

	public void setSlot(int[] slot) {
		this.slot = slot;
	}

	public double getU() {
		return u;
	}

	public void setU(double u) {
		this.u = u;
	}

	public double getMapToReduce() {
		return mapToReduce;
	}

	public void setMapToReduce(double mapToReduce) {
		this.mapToReduce = mapToReduce;
	}

	public double getQ() {
		return q;
	}

	public void setQ(double q) {
		this.q = q;
	}

	public static void main(String args[]){
		
		MinCost minCost=new MinCost();
		IloCplex cplex;
		
		try {
			cplex = new IloCplex();
			double x[]=minCost.minMapCost(cplex);
			double transVar[]=new double[x.length-1];
			
			for(int i=0;i<transVar.length;i++){
				transVar[i]=x[i];
			}
			
			int size=minCost.n;
			double[] inter=new double[size];
			for(int i=0;i<x.length-1;i++){
				inter[i%size]=inter[i%size]+x[i];
			}
			
			for(int i=0;i<size;i++){
				inter[i]=inter[i]*minCost.mapToReduce;
			}
			
			IloCplex cplex2 = new IloCplex();
			double y[]=minCost.minReduceCost(cplex2, inter);
	
			double comVar[]=new double[y.length-1];
		
			for(int i=0;i<comVar.length;i++){
				comVar[i]=y[i];
			}
			
			for(int i=0;i<transVar.length;i++){
				transVar[i]=x[i];
			}
			
			System.out.println("*******time:"+minCost.getTime(transVar,comVar));
			System.out.println("**********cost:"+minCost.getCost(transVar, comVar));
			
		} catch (IloException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//获得最小化时间情况下的成本消耗
		public double getCost(double[] transVar,double[] comVar){
			double sumCost=0.0;
			
			double[] aggrCost=new double[n*n];
			double[] mapCost=new double[n];
			double[] shufCost=new double[n*n];
			double[] redCost=new double[n];
			
			double sumAggrCost=0.0;
			double sumMapCost=0.0;
			double sumShufCost=0.0;
			double sumRedCost=0.0;
			
			//sumAggrCost
			for(int i=0;i<transVar.length;i++){
				if(i%n==i/n){
					aggrCost[i]=0;
				}else{
					aggrCost[i]=transVar[i]*bdCost[i/n][i%n];
				}
			}
			
			for(int i=0;i<transVar.length;i++){
				sumAggrCost=sumAggrCost+aggrCost[i];
			}
			
			//sumMapCost
			double[] inter=new double[n];
			for(int i=0;i<transVar.length;i++){
				inter[i%n]=inter[i%n]+transVar[i];
			}
			
			for(int i=0;i<n;i++){
				mapCost[i]=inter[i]*10*comCost[i]*tmap;
				sumMapCost=sumMapCost+mapCost[i];
			}
			
			//reduce阶段数据减半
			for(int i=0;i<n;i++){
				inter[i]=inter[i]*mapToReduce;
			}
			
			//sumShufCost
			for(int i=0;i<n;i++){
				for(int j=0;j<n;j++){
					if(i==j){
						shufCost[i*n+j]=0;
					}else{
						shufCost[i*n+j]=inter[i]*comVar[j]*bdCost[i][j];
						
					}
				}
			}
			
			for(int i=0;i<transVar.length;i++){
				sumShufCost=sumShufCost+shufCost[i];
			}
			
		
			for(int i=0;i<n;i++){
				redCost[i]=interSum*comVar[i]*10*comCost[i]*tred;
				sumRedCost=sumRedCost+redCost[i];
			}
			
			sumCost=sumAggrCost+sumMapCost+sumShufCost+sumRedCost;
			
			return sumCost;
		}
	
	//获得最小化成本情况下的传输时间
		public double getTransTime(double[] transVar,double[] comVar){
			double sumTime=0.0;
			
			double[] aggrTimeUp=new double[n];
			double[] aggrTimeDown=new double[n];
			double[] shufTimeUp=new double[n];
			double[] shufTimeDown=new double[n];
			
			double maxAggrTime=0.0;
			double maxShufTime=0.0;
			
			//maxAggrTime
			for(int i=0;i<n;i++){
				double upData=0;
				for(int j=0;j<n;j++){
					if(i==j){
						upData=upData+0;
					}else{
						upData=upData+transVar[i*n+j];
					}
				}
				aggrTimeUp[i]=(double)upData/bandWidthUp[i];
			}
			
			for(int i=0;i<n;i++){
				double downData=0;
				for(int j=0;j<n;j++){
					if(i==j){
						downData=downData+0;
					}else{
						downData=downData+transVar[j*n+i];
					}
				}
				aggrTimeDown[i]=(double)downData/bandWidthDown[i];
			}
			
			for(int i=0;i<n;i++){
				if(aggrTimeUp[i]>maxAggrTime){
					maxAggrTime=aggrTimeUp[i];
				}
				if(aggrTimeDown[i]>maxAggrTime){
					maxAggrTime=aggrTimeDown[i];
				}
			}
			
			//maxMapTime
			double[] inter=new double[n];
			for(int i=0;i<transVar.length;i++){
				inter[i%n]=inter[i%n]+transVar[i];
			}
			
			for(int i=0;i<n;i++){
				inter[i]=inter[i]*mapToReduce;
			}
			
			//maxShufTime
			for(int i=0;i<n;i++){
				double upData=0;
				for(int j=0;j<n;j++){
					if(i==j){
						upData=upData+0;
					}else{
						upData=upData+inter[i]*comVar[j];
					}
				}
				shufTimeUp[i]=(double)upData/bandWidthUp[i];
			}
			
			for(int i=0;i<n;i++){
				double downData=0;
				for(int j=0;j<n;j++){
					if(i==j){
						downData=downData+0;
					}else{
						downData=downData+inter[j]*comVar[i];
					}
				}
				shufTimeDown[i]=(double)downData/bandWidthDown[i];
			}
			
			for(int i=0;i<n;i++){
				if(shufTimeUp[i]>maxShufTime){
					maxShufTime=shufTimeUp[i];
				}
				if(shufTimeDown[i]>maxShufTime){
					maxShufTime=shufTimeDown[i];
				}
			}
			
			
			sumTime=maxAggrTime+maxShufTime;
			
			return sumTime;
		}
		//获得最小化成本情况下的时间消耗
		public double getComTime(double[] transVar,double[] comVar){
			double sumTime=0.0;
			
			double[] mapTime=new double[n];
			double[] redTime=new double[n];
			
			double maxMapTime=0.0;
			double maxRedTime=0.0;
			
			//maxMapTime
			double[] inter=new double[n];
			for(int i=0;i<transVar.length;i++){
				inter[i%n]=inter[i%n]+transVar[i];
			}
			
			for(int i=0;i<n;i++){
				mapTime[i]=tmap*inter[i]*u/slot[i];
			}
			
			for(int i=0;i<n;i++){
				if(mapTime[i]>maxMapTime){
					maxMapTime=mapTime[i];
				}
			}
			
			for(int i=0;i<n;i++){
				inter[i]=inter[i]*mapToReduce;
			}
			
			
			
			//maxRedTime
			for(int i=0;i<n;i++){
				redTime[i]=tred*interSum*comVar[i]*u/slot[i];
			}
			
			for(int i=0;i<n;i++){
				if(redTime[i]>maxRedTime){
					maxRedTime=redTime[i];
				}
			}
			
			
			sumTime=maxMapTime+maxRedTime;
			
			return sumTime;
		}
		
		//获得最小化时间情况下的时间消耗
		public double[] getDifferentStageTime(double[] transVar,double[] comVar){
			double[] aggrTimeUp=new double[n];
			double[] aggrTimeDown=new double[n];
			double[] mapTime=new double[n];
			double[] shufTimeUp=new double[n];
			double[] shufTimeDown=new double[n];
			double[] redTime=new double[n];
			
			double maxAggrTime=0.0;
			double maxMapTime=0.0;
			double maxShufTime=0.0;
			double maxRedTime=0.0;
			
			//maxAggrTime
			for(int i=0;i<n;i++){
				double upData=0;
				for(int j=0;j<n;j++){
					if(i==j){
						upData=upData+0;
					}else{
						upData=upData+transVar[i*n+j];
					}
				}
				aggrTimeUp[i]=(double)upData/bandWidthUp[i];
			}
			
			for(int i=0;i<n;i++){
				double downData=0;
				for(int j=0;j<n;j++){
					if(i==j){
						downData=downData+0;
					}else{
						downData=downData+transVar[j*n+i];
					}
				}
				aggrTimeDown[i]=(double)downData/bandWidthDown[i];
			}
			
			for(int i=0;i<n;i++){
				if(aggrTimeUp[i]>maxAggrTime){
					maxAggrTime=aggrTimeUp[i];
				}
				if(aggrTimeDown[i]>maxAggrTime){
					maxAggrTime=aggrTimeDown[i];
				}
			}
			
			//maxMapTime
			double[] inter=new double[n];
			for(int i=0;i<transVar.length;i++){
				inter[i%n]=inter[i%n]+transVar[i];
			}
			
			for(int i=0;i<n;i++){
				mapTime[i]=tmap*inter[i]*u/slot[i];
			}
			
			for(int i=0;i<n;i++){
				if(mapTime[i]>maxMapTime){
					maxMapTime=mapTime[i];
				}
			}
			
			for(int i=0;i<n;i++){
				inter[i]=inter[i]*mapToReduce;
			}
			
			//maxShufTime
			for(int i=0;i<n;i++){
				double upData=0;
				for(int j=0;j<n;j++){
					if(i==j){
						upData=upData+0;
					}else{
						upData=upData+inter[i]*comVar[j];
					}
				}
				shufTimeUp[i]=(double)upData/bandWidthUp[i];
			}
			
			for(int i=0;i<n;i++){
				double downData=0;
				for(int j=0;j<n;j++){
					if(i==j){
						downData=downData+0;
					}else{
						downData=downData+inter[j]*comVar[i];
					}
				}
				shufTimeDown[i]=(double)downData/bandWidthDown[i];
			}
			
			for(int i=0;i<n;i++){
				if(shufTimeUp[i]>maxShufTime){
					maxShufTime=shufTimeUp[i];
				}
				if(shufTimeDown[i]>maxShufTime){
					maxShufTime=shufTimeDown[i];
				}
			}
			
			//maxRedTime
			for(int i=0;i<n;i++){
				redTime[i]=tred*interSum*comVar[i]*u/slot[i];
			}
			
			for(int i=0;i<n;i++){
				if(redTime[i]>maxRedTime){
					maxRedTime=redTime[i];
				}
			}
			
			double[]  differentStageTime=new double[4];
			differentStageTime[0]=maxAggrTime;
			differentStageTime[1]=maxMapTime;
			differentStageTime[2]=maxShufTime;
			differentStageTime[3]=maxRedTime;
			
			return differentStageTime;
		}
	
				
		//获得最小化成本情况下的时间消耗
		public double getTime(double[] transVar,double[] comVar){
			double sumTime=0.0;
			
			double[] aggrTimeUp=new double[n];
			double[] aggrTimeDown=new double[n];
			double[] mapTime=new double[n];
			double[] shufTimeUp=new double[n];
			double[] shufTimeDown=new double[n];
			double[] redTime=new double[n];
			
			double maxAggrTime=0.0;
			double maxMapTime=0.0;
			double maxShufTime=0.0;
			double maxRedTime=0.0;
			
			//maxAggrTime
			for(int i=0;i<n;i++){
				double upData=0;
				for(int j=0;j<n;j++){
					if(i==j){
						upData=upData+0;
					}else{
						upData=upData+transVar[i*n+j];
					}
				}
				aggrTimeUp[i]=(double)upData/bandWidthUp[i];
			}
			
			for(int i=0;i<n;i++){
				double downData=0;
				for(int j=0;j<n;j++){
					if(i==j){
						downData=downData+0;
					}else{
						downData=downData+transVar[j*n+i];
					}
				}
				aggrTimeDown[i]=(double)downData/bandWidthDown[i];
			}
			
			for(int i=0;i<n;i++){
				if(aggrTimeUp[i]>maxAggrTime){
					maxAggrTime=aggrTimeUp[i];
				}
				if(aggrTimeDown[i]>maxAggrTime){
					maxAggrTime=aggrTimeDown[i];
				}
			}
			
			//maxMapTime
			double[] inter=new double[n];
			for(int i=0;i<transVar.length;i++){
				inter[i%n]=inter[i%n]+transVar[i];
			}
			
			for(int i=0;i<n;i++){
				mapTime[i]=tmap*inter[i]*u/slot[i];
			}
			
			for(int i=0;i<n;i++){
				if(mapTime[i]>maxMapTime){
					maxMapTime=mapTime[i];
				}
			}
			
			for(int i=0;i<n;i++){
				inter[i]=inter[i]*mapToReduce;
			}
			
			//maxShufTime
			for(int i=0;i<n;i++){
				double upData=0;
				for(int j=0;j<n;j++){
					if(i==j){
						upData=upData+0;
					}else{
						upData=upData+inter[i]*comVar[j];
					}
				}
				shufTimeUp[i]=(double)upData/bandWidthUp[i];
			}
			
			for(int i=0;i<n;i++){
				double downData=0;
				for(int j=0;j<n;j++){
					if(i==j){
						downData=downData+0;
					}else{
						downData=downData+inter[j]*comVar[i];
					}
				}
				shufTimeDown[i]=(double)downData/bandWidthDown[i];
			}
			
			for(int i=0;i<n;i++){
				if(shufTimeUp[i]>maxShufTime){
					maxShufTime=shufTimeUp[i];
				}
				if(shufTimeDown[i]>maxShufTime){
					maxShufTime=shufTimeDown[i];
				}
			}
			
			//maxRedTime
			for(int i=0;i<n;i++){
				redTime[i]=tred*interSum*comVar[i]*u/slot[i];
			}
			
			for(int i=0;i<n;i++){
				if(redTime[i]>maxRedTime){
					maxRedTime=redTime[i];
				}
			}
			
			sumTime=maxAggrTime+maxMapTime+maxShufTime+maxRedTime;
			
			return sumTime;
		}
		
    
	
	public double[] minReduceCost(IloCplex  cplex,double[] inter){
		int varSize=n;
		
		double[]  lb=new double[varSize];
		double[]  ub=new double[varSize];
		double[] sol=new double[varSize];
		double[] solAndReducecost=new double[varSize+1];
		
		
		//初始化下界
		for(int i=0;i<varSize;i++){
			lb[i]=0.0;
		}
		
		//初始化上界
		for(int i=0;i<varSize;i++){
			ub[i]=1.0;
		}
		IloNumVar[] x ;
		
	try {
		x = cplex.numVarArray(varSize, lb, ub);
		
		//定义并初始化目标函数
		double[] objvals=new double[varSize];
		
		//添加传输消耗
		for(int i=0;i<varSize;i++){//x
			for(int j=0;j<varSize;j++){//inter
				objvals[i]=objvals[i]+inter[j]*bdCost[j][i];
			}
		}
		
		//添加计算消耗
		for(int i=0;i<varSize;i++){
			objvals[i]=objvals[i]+comCost[i]*10*interSum*tred;
		}
	
			cplex.addMinimize(cplex.scalProd(x,objvals));
			
			//定义并初始化等式约束
			double[] coeff=new double[varSize];
		
			for(int i=0; i<varSize;i++){
					coeff[i]=1;
			}
			
			cplex.addEq(cplex.scalProd(x,coeff), 1);
			
			if ( cplex.solve() ) {
		        System.out.println("Solution status = " + cplex.getStatus());
		        System.out.println("Solution value  = " + cplex.getObjValue());

		        sol = cplex.getValues(x);
		        
		        for(int j=0;j<solAndReducecost.length-1;j++){
		        	solAndReducecost[j]=sol[j];
		        }
		       
		        solAndReducecost[solAndReducecost.length-1]=cplex.getObjValue();
		        
		        for (int j = 0; j <sol.length; ++j) {
		           System.out.println("Variable " + (j+1) + ": Value = " + sol[j]);
		        }
		      }
		      cplex.end();
			
		} catch (IloException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return solAndReducecost;
	}
	
	
	//最小化map阶段的传输和计算成本
	public double[] minMapCost(IloCplex cplex){
		
			int varSize=n*n;
			
			double[]  lb=new double[varSize];
			double[]  ub=new double[varSize];
			double[] sol=new double[varSize];
			double[] solAndMapcost=new double[varSize+1];
			
			//初始化下界
			for(int i=0;i<varSize;i++){
				lb[i]=0.0;
			}
			
			//初始化上界
			for(int i=0;i<varSize;i++){
				ub[i]=Double.MAX_VALUE;
			}
			
		try {
			IloNumVar[] x ;
			
			x = cplex.numVarArray(varSize, lb, ub);
			
			//定义并初始化目标函数
			double[] objvals=new double[varSize];
			
			//添加传输消耗
			for(int i=0;i<varSize;i++){
				objvals[i]=bdCost[i/n][i%n];
			}
			
			for(int i=0;i<varSize;i++){
				objvals[i]=objvals[i]+comCost[i%n]*10*tmap;
			}
			
			cplex.addMinimize(cplex.scalProd(x,objvals));
			
			//定义并初始化等式约束
			double[][] coeff=new double[n][varSize];
		
			for(int i=0; i<n;i++){
				for(int j=0;j<varSize;j++){
					if(j>=i*n&&j<(i+1)*n){
						coeff[i][j]=1;
					}
				}
			}
			
			for(int i=0; i<n;i++){
				cplex.addEq(cplex.scalProd(x,coeff[i]), input[i]);
			}
			
			
			if ( cplex.solve() ) {
		        System.out.println("Solution status = " + cplex.getStatus());
		        System.out.println("Solution value  = " + cplex.getObjValue());

		        sol = cplex.getValues(x);
		        
		        for(int j=0;j<solAndMapcost.length-1;j++){
		        	solAndMapcost[j]=sol[j];
		        }
		       
		        solAndMapcost[solAndMapcost.length-1]=cplex.getObjValue();
		        
		        for (int j = 0; j <sol.length; ++j) {
		           System.out.println("Variable " + (j+1) + ": Value = " + sol[j]);
		        }
		        
		      }
		      cplex.end();
			
			
		} catch (IloException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return solAndMapcost;
	}
}
