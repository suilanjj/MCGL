package minTime;

import ilog.concert.IloException;
import ilog.concert.IloNumVar;
import ilog.cplex.IloCplex;

//分别计算map和reduce两阶段的最小化时间的任务放置策略
public class MinTime {
	
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
	public MinTime(int n, double[] comCost, double[][] bdCost, double[] input,
			double[] bandWidthUp, double[] bandWidthDown, double interSum,
			int[] slot, double u, double mapToReduce, double q, double tmap, double tred) {
		super();
		this.n = n;
		this.input = input;
		this.bandWidthUp = bandWidthUp;
		this.bandWidthDown = bandWidthDown;
		this.comCost = comCost;
		this.bdCost = bdCost;
		this.slot = slot;
		this.u = u;
		this.q = q;
		this.mapToReduce = mapToReduce;
		this.interSum = interSum;
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
	public MinTime() {
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

	public double getQ() {
		return q;
	}

	public void setQ(double q) {
		this.q = q;
	}

	public double getMapToReduce() {
		return mapToReduce;
	}

	public void setMapToReduce(double mapToReduce) {
		this.mapToReduce = mapToReduce;
	}

	public double getInterSum() {
		return interSum;
	}

	public void setInterSum(double interSum) {
		this.interSum = interSum;
	}

	public static void main(String args[]){
		
		try {
			IloCplex cplex = new IloCplex();
		
			MinTime minTime=new MinTime();
			
			double x[]=minTime.minMapTime(cplex);
			
			int size=minTime.n;
			double[] inter=new double[size];
			for(int i=0;i<x.length-2;i++){
				inter[i%size]=inter[i%size]+x[i];
			}
			
			for(int i=0;i<size;i++){
				inter[i]=inter[i]*minTime.mapToReduce;
			}
			
			IloCplex cplex2 = new IloCplex();
			double[] y=minTime.minReduceTime(cplex2, inter);
			
			double[] transVar=new double[minTime.n*minTime.n];
			double[] comVar=new double[minTime.n];
			
			for(int i=0;i<transVar.length;i++){
				transVar[i]=x[i];
			}
			
			for(int i=0;i<comVar.length;i++){
				comVar[i]=y[i];
			}
			
			System.out.println("*******cost:"+minTime.getCost(transVar,comVar));
			System.out.println("*******time:"+minTime.getTime(transVar,comVar));
			
		} catch (IloException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} // creat a model
	}
		//获得最小化时间情况下的传输成本
		public double getTransCost(double[] transVar,double[] comVar){
			double sumCost=0.0;
			
			double[] aggrCost=new double[n*n];
			double[] shufCost=new double[n*n];
			
			double sumAggrCost=0.0;
			double sumShufCost=0.0;
			
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
			
			sumCost=sumAggrCost+sumShufCost;
			
			return sumCost;
		}
		
		
		
		//获得最小化时间情况下的时间消耗
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
		
		//获得不同阶段的时间消耗
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
		
		
		//获得最小化时间情况下的计算成本
		public double getComCost(double[] transVar,double[] comVar){
			double sumCost=0.0;
			
			double[] mapCost=new double[n];
			double[] redCost=new double[n];
			
			double sumMapCost=0.0;
			double sumRedCost=0.0;
			
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
			
			//sumRedCost
			for(int i=0;i<n;i++){
				redCost[i]=interSum*comVar[i]*10*comCost[i]*tred;
				sumRedCost=sumRedCost+redCost[i];
			}
			
			sumCost=sumMapCost+sumRedCost;
			
			return sumCost;
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
		
		//sumRedCost
		for(int i=0;i<n;i++){
			redCost[i]=interSum*comVar[i]*10*comCost[i]*tred;
			sumRedCost=sumRedCost+redCost[i];
		}
		
		sumCost=sumAggrCost+sumMapCost+sumShufCost+sumRedCost;
		
		return sumCost;
	}

	//最小化reduce阶段的时间消耗
	public double[] minReduceTime(IloCplex cplex,double[] inter){
		
		int varSize=n+2;
		double[]   lb= new double[varSize];
		double[]   ub=new double[varSize];
		double[] sol=new double[varSize];
		double[] solAndReducetime=new double[varSize+1];
		
		//初始化下界
		for(int i=0;i<varSize;i++){
			lb[i]=0.0;
		}
		
		//初始化上界
		for(int i=0;i<varSize;i++){
			if(i<n){
				ub[i]=1.0;
			}else{
				ub[i]=Double.MAX_VALUE;
			}
		}
	
		IloNumVar[] x ;
		try {
			x = cplex.numVarArray(varSize, lb, ub);
			
			//定义并初始化目标函数
			double[] objvals=new double[varSize];
			
			for(int i=0;i<varSize;i++){
				if(i<n){
					objvals[i]=0;
				}else{
					objvals[i]=1;
				}
			}
			
			cplex.addMinimize(cplex.scalProd(x,objvals));
			
			//定义并初始化等式约束
			double[] coeff=new double[varSize];
		
			for(int i=0; i<varSize;i++){
				if(i<n){
					coeff[i]=1;
				}else{
					coeff[i]=0;
				}
			}
			
			for(int i=0; i<n;i++){
				cplex.addEq(cplex.scalProd(x,coeff), 1);
			}
			
			//定义并初始化不等式约束
			double[][] leeff=new double[n*2+n][varSize];
			
			
			for(int i=0;i<n*2;i++){	
				int l=i%n;   //第l个数据中心的
				if(i<n){		
					for(int j=0;j<varSize;j++){
						
						int m=j%n;  //
						
						if(j==varSize-2){
							leeff[i][j]=-1;
						}else if(j<n&&l!=m){
							leeff[i][j]=(double)inter[l]/bandWidthUp[l];
						}else{
							leeff[i][j]=0;
						}
					}//for
				}else{
					for(int j=0;j<varSize;j++){
						
						int m=j%n;
						
						if(j==varSize-2){
							leeff[i][j]=-1;
						}else if(m==l&&j<varSize-2){
							
								leeff[i][j]=(double)(interSum-inter[l])/bandWidthDown[l];
						}else{
							leeff[i][j]=0;
						}
					}//for
				}//else
			}//for
			
			for(int i=n*2;i<n*2+n;i++){  //计算时间约束
				int m=i%(n*2);
				for(int j=0;j<varSize;j++){
					if(j==varSize-1){
						leeff[i][j]=-1;
					}else if(m==j){
						leeff[i][j]=tred*interSum*((double)1*u/slot[m]);
					}else{
						leeff[i][j]=0;
					}
				}
			}
			
			for(int i=0;i<n*2+n;i++){
				cplex.addLe(cplex.scalProd(x,leeff[i]), 0.0);
			}
			
			if ( cplex.solve() ) {
		        System.out.println("Solution status = " + cplex.getStatus());
		        System.out.println("Solution value  = " + cplex.getObjValue());

		        sol = cplex.getValues(x);
		        
		        for(int j=0;j<solAndReducetime.length-1;j++){
		        	solAndReducetime[j]=sol[j];
		        }
		       
		        solAndReducetime[solAndReducetime.length-1]=cplex.getObjValue();
		        
		        for (int j = 0; j <sol.length; ++j) {
		           System.out.println("Variable " + (j+1) + ": Value = " + sol[j]);
		        }
		      }
		      cplex.end();
			
		}catch (IloException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return solAndReducetime;
	}
    
	
	
	//最小化map阶段的时间消耗
	public  double[]  minMapTime(IloCplex cplex ){
		//定义其他变量
		int  varSize=n*n+2;
		double[]   lb= new double[varSize];
		double[]   ub=new double[varSize];
		double[] sol=new double[varSize];
		double[] solAndmaptime=new double[varSize+1];
		
		//初始化上界
		for(int i=0;i<varSize;i++){
			lb[i]=0.0;
		}
		
		//初始化下界
		for(int i=0;i<n;i++){
			for(int j=0;j<n;j++){
				ub[i*n+j]=input[i];
			}
			ub[n*n]=Double.MAX_VALUE;
			ub[n*n+1]=Double.MAX_VALUE;
		}
		
		//定义决策变量
	    IloNumVar[] x;
		try {
			x = cplex.numVarArray(varSize, lb, ub);
		
	    //定义并初始化目标函数
		double[] objvals=new double[varSize];
		
		for(int i=0;i<n;i++){
			for(int j=0;j<n;j++){
				objvals[i*n+j]=0;
			}
			objvals[n*n]=1;
			objvals[n*n+1]=1;
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
			coeff[i][varSize-1]=0;
			coeff[i][varSize-2]=0;
		}
		
		for(int i=0; i<n;i++){
			cplex.addEq(cplex.scalProd(x,coeff[i]), input[i]);
		}
		
		//定义并初始化不等式约束
		double[][] leeff=new double[n*2+n][varSize];
		
		for(int i=0;i<n*2;i++){	
			int l=i%n;   //第l个数据中心的
			if(i<n){		
				for(int j=0;j<varSize;j++){
					int m=j%n;  //
					
					if(j==varSize-2){
						leeff[i][j]=-1;
					}else if(j>=l*n&&j<(l+1)*n&&j<varSize-2){
						if(m==l){
							leeff[i][j]=0;
						}else{
							leeff[i][j]=(double)1/bandWidthUp[l];
						}
					}else{
						leeff[i][j]=0;
					}
				}//for
			}else{
				for(int j=0;j<varSize;j++){
					int k=j/n;  
					int m=j%n;
					
					if(j==varSize-2){
						leeff[i][j]=-1;
					}else if(m==l&&j<varSize-2){
						if(m==k){
							leeff[i][j]=0;
						}else{
							leeff[i][j]=(double)1/bandWidthDown[l];
						}
					}else{
						leeff[i][j]=0;
					}
				}//for
			}//else
		}//for
		
		for(int i=n*2;i<n*2+n;i++){  //计算时间约束
			int m=i%(n*2);
			for(int j=0;j<varSize;j++){
				if(j==varSize-1){
					leeff[i][j]=-1;
				}else if(j%n==m&&j<varSize-2){
					leeff[i][j]=tmap*((double)1*u/slot[m]);
				}
			}
		}
		
		for(int i=0;i<n*2+n;i++){
			cplex.addLe(cplex.scalProd(x,leeff[i]), 0.0);
		}
		
		if ( cplex.solve() ) {
	        System.out.println("Solution status = " + cplex.getStatus());
	        System.out.println("Solution value  = " + cplex.getObjValue());

	        sol = cplex.getValues(x);
	        
	        for(int j=0;j<solAndmaptime.length-1;j++){
	        	solAndmaptime[j]=sol[j];
	        }
	       
	        solAndmaptime[solAndmaptime.length-1]=cplex.getObjValue();
	        
	        for (int j = 0; j <sol.length; ++j) {
	           System.out.println("Variable " + (j+1) + ": Value = " + sol[j]);
	        }
	      }
	      cplex.end();
		} catch (IloException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return solAndmaptime;
	}
	
}
