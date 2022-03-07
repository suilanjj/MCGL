package minCostConsiderTime;

import ilog.concert.IloException;
import ilog.cplex.IloCplex;
import minCost.MinCost;
import minTime.MinTime;

public class MinCostConsiderTime {
		public static void main(String args[]){
			
			 int n=3;
			 double[] comCost={0.06,0.01,0.025};
			 double[][] bdCost={{0,0.5,5},{0.1,0,0.2},{0.2,0.3,0}};
			 double[] input={20,30,50};
			
			 double[] bandWidthUp={5,1,2};
			 double[] bandWidthDown={5,1,5};
			
			 double interSum=50;
			
			 int[]  slot={40,10,20};
			
			//每1GB数据需要多少slot
			  double  u=10;
			
			//中间数据和输入数据的比值
			 double   mapToReduce=0.5;
			 
			//每一个map花费的时间
			double   tmap=2;
				
			//每一个reduce花费的时间
			double   tred=1;
			
			//完成单个map任务和reduce任务时间比值
			 double   q=2;
			 
			 
			MinCost minCost=new MinCost(n, comCost, bdCost, input,bandWidthUp, bandWidthDown, interSum,slot, u, mapToReduce, q,tmap,tred);
			MinTime minTime=new MinTime(n, comCost, bdCost, input,bandWidthUp, bandWidthDown, interSum,slot, u, mapToReduce, q,tmap,tred);
			IloCplex cplex;
			
			try {
				//最小化成本策略
				cplex = new IloCplex();
				double transVarCost[]=minCost.minMapCost(cplex);
				
				int size=minCost.n;
				double[] interCost=new double[size];
				for(int i=0;i<transVarCost.length-1;i++){
					interCost[i%size]=interCost[i%size]+transVarCost[i];
				}
				
				for(int i=0;i<size;i++){
					interCost[i]=interCost[i]*minCost.mapToReduce;
				}
				
				
				IloCplex cplexCost2 = new IloCplex();
				double comVarCost[]=minCost.minReduceCost(cplexCost2, interCost);
				double timeofMinCost=minCost.getTime(transVarCost,comVarCost);
				
				//最小化时间策略
				IloCplex cplexTime = new IloCplex();
				double xTime[]=minTime.minMapTime(cplexTime);
				
				//int size=minTime.n;
				double[] interTime=new double[size];
				for(int i=0;i<xTime.length-2;i++){
					interTime[i%size]=interTime[i%size]+xTime[i];
				}
				
				for(int i=0;i<size;i++){
					interTime[i]=interTime[i]*minTime.mapToReduce;
				}
				
				IloCplex cplexTime2 = new IloCplex();
				double[] y=minTime.minReduceTime(cplexTime2, interTime);
				
				double[] transVarTime=new double[minTime.n*minTime.n];
				double[] comVarTime=new double[minTime.n];
				
				for(int i=0;i<transVarTime.length;i++){
					transVarTime[i]=xTime[i];
				}
				for(int i=0;i<comVarTime.length;i++){
					comVarTime[i]=y[i];
				}
				
				double costofMinTime=minTime.getCost(transVarTime,comVarTime);
				
				double[] transVarChange=new double[transVarTime.length];
				double[] comVarChange=new double[comVarTime.length];
				
				double changeSize=0;
				
				for(int i=0;i<transVarTime.length;i++){
					if(transVarCost[i]>transVarTime[i]){
						transVarChange[i]=transVarCost[i]-(transVarCost[i]-transVarTime[i])*changeSize;
					}else{
						transVarChange[i]=transVarCost[i]+(transVarTime[i]-transVarCost[i])*changeSize;
					}
				}
				
				//System.out.println("reduce");
				for(int i=0;i<comVarTime.length;i++){
					if(comVarCost[i]>comVarTime[i]){
						comVarChange[i]=comVarCost[i]-(comVarCost[i]-comVarTime[i])*changeSize;
					}else{
						comVarChange[i]=comVarCost[i]+(comVarTime[i]-comVarCost[i])*changeSize;
					}
				}
				
				System.out.println("*******time:"+minTime.getTime(transVarChange, comVarChange));
				System.out.println("*********cost:"+minTime.getCost(transVarChange, comVarChange));
				
			} catch (IloException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
}
