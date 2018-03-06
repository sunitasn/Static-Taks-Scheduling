package distributedscheduling;

import java.util.Vector;
import java.util.Comparator;
import java.util.PriorityQueue;
import static java.lang.System.out;
import java.util.Arrays;


/**
 * @author apurv verma
 */
public class SimulatorEngine {

    /*p[i] represents all tasks submitted to the ith machine*/
    public PriorityQueue<Task> p[];   

    /*Comparator for tasks*/
    private Comparator<Task> comparator;

    /*The total number of tasks*/
    int n;

    /*The number of machines*/
    int m;

    /*The poisson arrival rate*/
    double lambda;

    /*Meta-task set size*/
    int S;

    /*Arrival time of tasks*/
    public int arrivals[];

    /*ETC matrix*/
   // public int etc[][];

    /*Machine availability time, the time at which machine i finishes all previously assigned tasks.*/
    public int mat[];

    private SchedulingEngine eng;

    TaskHeterogeneity TH;
    MachineHeterogeneity MH;
    public int etc[][];
    // refinery example
  // public int etc[][] ={{55,28 ,73},{45 ,41 ,30} ,{112 ,65 ,12}, {137 ,119 ,64} ,{124 ,48 ,59} ,{172 ,136 ,51} ,{138 ,23 ,122} ,{27 ,150 ,16} ,{26 ,94 ,33} ,{100 ,126 ,66} ,
    //    {161, 87, 107,}, {175, 36, 25,}, {20, 65, 90,},{ 121, 130, 107}, {170, 116, 53}};
 // erfinery example*/

 /*  public int etc[][]={{  694,  604, 594},{ 179, 279, 309},{237, 237, 532},{ 391, 469, 157},{401, 451, 151},{75, 667, 593},
{ 593, 75,223},{ 545, 69 ,545},{109, 25, 31},{35, 52, 18},{ 433, 11, 217},{ 78, 386, 1},{ 1, 271, 631},{ 163, 111, 244},{ 31, 271, 101}};
*/
   //public int etc[][]={{391, 703, 547},{115,229,191},{523,610,11},{205,137,137},{193,257,257},{51,201,76},{57,141,225},
//{ 229,191,101},{478,425,54},{191,15,476},{414,296,119},{137,273,273},{39,77,229},{ 5, 21, 9},
     //   {172, 20, 100}, {157, 703, 10}};


// emin example
     // public int etc[][] ={{229,153,533,609},{337,169,253,673},{514,115,115,286},{273,307,137,171}};
    
    /*For calculating avg completion time*/
    long sigma;

    /*For calculating makespan*/
    long makespan;
    public double averageResourceUtil = -1;
    public double loadBalanceLevel  = -1;

    public SimulatorEngine(int NUM_MACHINES, int NUM_TASKS, double ARRIVAL_RATE, int metaSetSize,Heuristic HEURISTIC, TaskHeterogeneity th, MachineHeterogeneity mh){
        
        sigma=0;
        makespan=0;

        MH=mh;
        TH=th;
        n=NUM_TASKS;
        S=metaSetSize;
        m=NUM_MACHINES;
        lambda=ARRIVAL_RATE;
        comparator=new TaskComparator();
        p=new PriorityQueue[m];      
        eng=new SchedulingEngine(this,HEURISTIC);

        for(int i=0;i<p.length;i++)
            p[i]=new PriorityQueue<Task>(5,comparator);
        
        generateRandoms(false);
        mat=new int[m];
    }

    private void generateRandoms(boolean s){
        arrivals=new ArrivalGenerator(n,lambda).getArrival();
         etc=new ETCGenerator(m,n,TH,MH).getETC();
        int i=0;
        // sorting code
       if(s==true){
        int temp[]= new int[(n*m)];
              for (int r = 0; r < n; r ++)
                for (int c = 0; c < m; c ++)
                           temp[i++]= etc[r][c] ;
       i--;
          Arrays.sort(temp);
          i=0;
       for (int r = 0; r < n; r ++)
                for (int c = 0; c < m; c ++)
                           etc[r][c]= temp[i++] ;
     
           System.out.println( "\n \t ETC ");
     for(i=0;i<n; i++)
               {  if(i==0)
                   System.out.print("{{");
               else 
                   System.out.print("{");
                   for(int j=0;j <m; j++)
                   {  if(j==0)
                       System.out.print( etc[i][j]);
                     else
                       System.out.print("," + etc[i][j]);
                   }
                   System.out.print("},");
                }
     }
}
    public void newSimulation(boolean generateRandoms){
        makespan=0;
        sigma=0;
        if(generateRandoms){
            generateRandoms(true);
        }
        for(int i=0;i<m;i++){
            mat[i]=0;
            p[i].clear();
        }
    }

    public void setHeuristic(Heuristic h){
        this.eng.h=h;
    }

    public long getMakespan() {
        return makespan;
    }

    public int[] getArrivals() {
        return arrivals;
    }

    public int[][] getEtc() {
        return etc;
    }

    public void mapTask(Task t, int machine){
        t.set_eTime(etc[t.tid][machine]);
        t.set_cTime( mat[machine]+etc[t.tid][machine] );
        p[machine].offer(t);
        mat[machine]=t.cTime;
    }

    public void simulate(){
        /*tick represents the current time*/
        int tick=0;

        Vector<Task> metaSet=new Vector<Task>(S);
        int i1=0;
        int i2=S;

        /*Initialization*/
        /*Add the first S tasks to the meta set and schedule them*/
        for(int i=i1;i<i2;i++){
            Task t=new Task(arrivals[i],i);
            metaSet.add(t);
        }
        i1=i2;
        i2=(int) min(i1+S, arrivals.length);
        /*Set tick to the time of the first mapping event*/
        tick=arrivals[i1-1];
        eng.schedule(metaSet,tick);

        /*Set tick to the time of the next mapping event*/
        tick=arrivals[i2-1];

        /*Simulation Loop*/
        do{

            /*Set the current tick value*/
            if(i2==i1){
                tick=Integer.MAX_VALUE;                
                /*Remove all the completed tasks from all the machines*/
                removeCompletedTasks(tick);
                break;
            }
            else{
                /*The time at which the next mapping event takes place*/
                tick=arrivals[i2-1];
                /*Remove all the completed tasks from all the machines*/
                removeCompletedTasks(tick);
            }
            /**/
            
            /*Collect next S OR (i2-i1) tasks to the meta set and schedule them*/
            metaSet=new Vector<Task>(i2-i1);

            for(int i=i1;i<i2;i++){
                Task t=new Task(arrivals[i],i);
                metaSet.add(t);
            }
            eng.schedule(metaSet, tick);
            /**/

            /*Set values for next iteration.*/
            i1=i2;
            i2=(int) min(i1+S, arrivals.length);
            /**/

        }while(!discontinueSimulation());
    }

    private void removeCompletedTasks(int currentTime){
        for(int i=0;i<this.m;i++){
            if(!p[i].isEmpty()){
                Task t=p[i].peek();               
                while(t.cTime<=currentTime){                 
                    sigma+=t.cTime;
                    makespan=max(makespan,t.cTime);
                    //out.println("Removing task "+t.tid+" at time "+currentTime);////////////////////////
                    t=p[i].poll();
                    if(!p[i].isEmpty())
                        t=p[i].peek();
                    else
                        break;
                }
            }
        }
    }

    private boolean discontinueSimulation(){
        boolean result=true;
        for(int i=0;i<this.m && result;i++)
            result=result && p[i].isEmpty();
        return result;
    }

    private long max(long a,long b){
        if(a>b)
            return a;
        else
            return b;
    }

    private long min(long a,long b){
        if(a<b)
            return a;
        else
            return b;
    }

   
}
