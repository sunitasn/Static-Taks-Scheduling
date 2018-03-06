package distributedscheduling;

import java.util.Vector;
import java.util.Iterator;
import static java.lang.System.out;
import java.util.Arrays;

public class SchedulingEngine {

    Heuristic h;
    SimulatorEngine sim;
    
    public SchedulingEngine(SimulatorEngine sim,Heuristic heuristic){
        h=heuristic;
        this.sim=sim;
    }

    public void schedule(Vector<Task> metaSet,int currentTime){

        /*If any machine has zero assigned tasks then set mat[] for that machine to be the current time.*/
        for(int i=0;i<sim.m;i++){
            if(sim.p[i].isEmpty()){
                sim.mat[i]=currentTime;
            }
        }

        
if(h == Heuristic.Minmin)
  schedule_MinMin(metaSet,currentTime);
// erifnery setup
  if (h == Heuristic.ERefinery)
       schedule_ERefinery(metaSet,currentTime); 

 if (h == Heuristic.Refinery)
     schedule_Refinery(metaSet,currentTime);
  
    }
// Original MINMIN with modified refinery
    private void schedule_ERefinery(Vector<Task> metaSet, int currentTime){

        /*We do not actually delete the task from the meta-set rather mark it as removed*/

        System.out.println(" Efficient Refinery Heuristic ");
          System.out.println(" \t First Phase ");
        boolean[] isRemoved=new boolean[metaSet.size()];
        double[] ru = new double[sim.m];
        double[] totalExec = new double[sim.m];
        double tempSum = 0;
       // int[][] machineTasks = new int[sim.m][metaSet.size()];
        int machineTasks[] = new int[sim.m];
        double balanceDev = 0;
        double tempSum2 = 0;
        /*Matrix to contain the completion time of each task in the meta-set on each machine.*/
        int c[][]=schedule_MinMinHelper(metaSet);
        int i=0;

        int[] tasksSeries = new int[metaSet.size()];
        int[][] machinesAndTasks = new int[sim.m][metaSet.size()];
        int[] machineSeries = new int[metaSet.size()];

        for(int j=0;j<metaSet.size();j++){
            tasksSeries[j] = -1;
            machineSeries[j] = -1;
            for(int k=0;k<sim.m;k++){
                machinesAndTasks[k][j] = -1;
            }
        }

        int tasksRemoved=0;
        do{
            int minTime=Integer.MAX_VALUE;
            int machine=-1;
            int taskNo=-1;
              /*Find the task in the meta set with the earliest completion time and the machine that obtains it.*/
            for(i=0;i<metaSet.size();i++){
                if(isRemoved[i])continue;
                for(int j=0;j<sim.m;j++){
                    if(c[i][j]< minTime){
                        minTime=c[i][j];
                        machine=j;
                        taskNo=i;
                    }
                }
            }

             Task t=metaSet.elementAt(taskNo);
             ru[machine] += sim.etc[metaSet.get(taskNo).tid][machine];
             sim.mapTask(t, machine);
            /*Mark this task as removed*/
             isRemoved[taskNo]=true;

            /*Update c[][] Matrix for other tasks in the meta-set*/
            for(i=0;i<metaSet.size();i++){
                if(isRemoved[i])continue;
                else{
                    c[i][machine]=sim.mat[machine]+sim.etc[metaSet.get(i).tid][machine];
                }
            }
             out.println("Assigning task "+t.tid+" to machine "+machine+". Completion time = "+t.cTime);
             // task order how task are assign
             tasksSeries[tasksRemoved] = taskNo;
             // which task is assing to which
             machineSeries[tasksRemoved] = machine;
             machinesAndTasks[machine][machineTasks[machine]] = taskNo;
             machineTasks[machine]++;
             totalExec[machine] = t.cTime;
             tasksRemoved++;

        }while(tasksRemoved!=metaSet.size());
        System.out.println("\n \t Second Phase \n");
       // code of ER
        // finding highest task based on their completion time
        boolean canBeStopped  = false;
        //int canBeStoppedInt = 0;
        while(canBeStopped==false ){

            int highestMachine = 0;
            double highestTime = -1;
            for(int m=0;m<sim.m;m++){
                if(totalExec[m] > highestTime){
                    highestMachine = m ;
                    highestTime = totalExec[m];
                }
            }

 // higest make span is store in highestMakeSpan1 and hm
            double highestMakespan = 0;
            double highestMakespan1 = highestTime;
             double hm = highestTime;
            //int exchangeNumber = 0;
             // this all variable are used to store move related information
             int fromTask1 =-1;
             int toTask1 =-1;
             int toMachine1 = -1;
             // this all variable are used to store swap realted information
            int fromTask = -1;
            int fromIndex = -1;
            int toTask = -1;
            // selection method_0 means move
            // selection method_1 means swap
            int selectionMethod_0 = -1;
            int selectionMethod_1 = -1;
            int selectionMethod = -1;
            int toMachine =-1;

            // machineandTasks keep the task which is assigned to machine and its etc time
            // machineTasks keeps all the task in order of assignment it will check based on machine number and it keeps the tasknumber
            // highestMachine is the machine which has highest completion time
            for(int a=0;a<machineTasks[highestMachine];a++){
                int taskA = machinesAndTasks[highestMachine][a];
                for(int m=0;m<sim.m;m++){
                    if(m == highestMachine) continue;
                   // for(int b=0; b<metaSet.size(); b++){
                      //  if(b == taskA) continue;
                       // out.println("M = taskA = " + m + taskA);
                    double totalExec1 = totalExec[m] + sim.etc[metaSet.get(taskA).tid][m];
                    double totalExec2 = totalExec[highestMachine] - sim.etc[metaSet.get(taskA).tid][highestMachine] ;
                    if(totalExec1<highestTime && totalExec2<highestTime){
                        if(totalExec1>totalExec2)  
                            highestMakespan = totalExec1;
                           else        
                              highestMakespan = totalExec2;
                                                      
                        if(highestMakespan < highestMakespan1){
                            highestMakespan1 = highestMakespan;
                            fromTask = taskA;
                            fromIndex = a;
                            toTask = taskA;
                            toMachine = m;
                            selectionMethod_0 = 0;
                        }
                    }
                }
             }
            System.out.println(" new make span from  move method  =  " + highestMakespan1 + " Task =  "+ fromTask
                           + " toMachine = " + toMachine );
// code for swaping
            for(int a=0;a<machineTasks[highestMachine];a++){
                int taskA = machinesAndTasks[highestMachine][a];
                for(int m=0;m<sim.m;m++){
                    if(m == highestMachine) continue;
                    for(int b=0; b<machineTasks[m]; b++){
                        int taskB = machinesAndTasks[m][b];
                        double totalExec1 = totalExec[m] + sim.etc[metaSet.get(taskA).tid][m] - sim.etc[metaSet.get(taskB).tid][m];
                        double totalExec2 = totalExec[highestMachine] - sim.etc[metaSet.get(taskA).tid][highestMachine] + sim.etc[metaSet.get(taskB).tid][highestMachine];
                        if(totalExec1<highestTime && totalExec2<highestTime){
                            if(totalExec1>totalExec2)  highestMakespan = totalExec1;
                            else highestMakespan = totalExec2;
                            if(highestMakespan < hm){
                               //highestMakespan1 = highestMakespan;
                                hm=highestMakespan;
                                fromTask1 = taskA;
                                toTask1 = taskB;
                                toMachine1 = m;
                                selectionMethod_1 = 1;
                            }
                        }
                    }
                }
                 
            }

             System.out.println(" new make span from swap method = "+hm + " Task = "+ fromTask1 +" to Task = " +toTask1);


            // from move highestmakespan1 bring and from swap hm bring
             // who ever is giving better result is choosen
            if (hm < highestMakespan1)
            { selectionMethod = selectionMethod_1;
                highestMakespan1 = hm;
                fromTask = fromTask1;
                toTask= toTask1;
                toMachine = toMachine1;
            }
            else if(hm > highestMakespan1)
                selectionMethod = selectionMethod_0;
            else
                selectionMethod = -1;

             if(selectionMethod ==0)
                 out.println(" move method has less make span");
             else
                 out.println(" swap method has less make span");
             
            System.out.println(" Lowest Makespan1 ="+highestMakespan1 + " fromTask ="+ fromTask +" fromIndex =" +fromIndex
                            + " toTask ="+ toTask+  " toMachine =" + toMachine );
            // move work
            if(selectionMethod == 0){
                 
// increment the number of tasks to machine
                machineTasks[toMachine]++;
                // the number of task assigned to new machine
                int a1 = machineTasks[toMachine];
                // assingend that task to to machine
                machinesAndTasks[toMachine][a1-1] = toTask;
               
                int b1= -1;
                // highestmachine task is removed
                machinesAndTasks[highestMachine][fromIndex] = machinesAndTasks[highestMachine][machineTasks[highestMachine]-1];
                // task count of highest m/c is decrement
                machineTasks[highestMachine]--;
                // make changes in taskSeries
                for(int n =0;n<metaSet.size();n++){
                    if(tasksSeries[n] == fromTask) b1=n;
                    if(b1!=-1) break;
                }
                tasksSeries[b1] = toTask;
                // make changes is machine series
                machineSeries[b1] = toMachine;
                // calculate resoures utilization and load balance
                ru[highestMachine] -= sim.etc[metaSet.get(fromTask).tid][highestMachine] ;
                ru[toMachine] += sim.etc[metaSet.get(toTask).tid][toMachine] ;
                totalExec[highestMachine] -= sim.etc[metaSet.get(fromTask).tid][highestMachine] ;
                totalExec[toMachine] += sim.etc[metaSet.get(toTask).tid][toMachine] ;
                }

            // swap work
             else if(selectionMethod == 1){
                int a1 = -1;
                int b1 = -1;
                int a = -1;
                int b =-1;
                for(int n =0;n<machineTasks[highestMachine];n++){
                    if(machinesAndTasks[highestMachine][n] == fromTask) a=n;
                }
                for(int n =0;n<machineTasks[toMachine];n++){
                    if(machinesAndTasks[toMachine][n] == toTask) b=n;
                }
                machinesAndTasks[highestMachine][a] = toTask ;
                machinesAndTasks[toMachine][b] = fromTask ;
                for(int n =0;n<metaSet.size();n++){
                    if(tasksSeries[n] == fromTask) a1=n;
                    if(a1!=-1) break;
                }
                for(int n =0;n<metaSet.size();n++){
                    if(tasksSeries[n] == toTask) b1=n;
                    if(b1!=-1) break;
                }
                tasksSeries[a1] = toTask;
                tasksSeries[b1] = fromTask;
                a = fromTask;
                b = toTask;
                ru[highestMachine] += sim.etc[metaSet.get(b).tid][highestMachine] - sim.etc[metaSet.get(a).tid][highestMachine];
                ru[toMachine] += sim.etc[metaSet.get(a).tid][toMachine] - sim.etc[metaSet.get(b).tid][toMachine];
                totalExec[highestMachine] += sim.etc[metaSet.get(b).tid][highestMachine] - sim.etc[metaSet.get(a).tid][highestMachine];
                totalExec[toMachine] += sim.etc[metaSet.get(a).tid][toMachine] - sim.etc[metaSet.get(b).tid][toMachine];
               // sim.mat[highestMachine] += sim.etc[metaSet.get(b).tid][highestMachine] - sim.etc[metaSet.get(a).tid][highestMachine];
                //sim.mat[toMachine] += sim.etc[metaSet.get(a).tid][toMachine] - sim.etc[metaSet.get(b).tid][toMachine];
             } // end of selection method -1

             else if(selectionMethod == -1){
                 canBeStopped = true;
             }
           // canBeStoppedInt++;

        }

       // for(int h1=0;h1<metaSet.size();h1++) out.println(machineSeries[h1]);
      //  for(int h1=0;h1<metaSet.size();h1++) out.println(tasksSeries[h1]);
       out.println(" \t Final schedule \n");
        for(int h1=0;h1<sim.m;h1++) sim.mat[h1] = 0;

        for(int n=0;n<metaSet.size();n++){
            int t1 = tasksSeries[n];
            int t2 =  machineSeries[n];
            Task t3=metaSet.elementAt(t1);
            sim.mapTask(t3, t2);
            out.println("Assigning task " + t3.tid  + " to machine " + t2 + ". Completion time = "+t3.cTime);
        }


        for(i=0;i<sim.m;i++){
             if(totalExec[i]==0){
                 ru[i] = 0;
             }
             else{
                 ru[i] = (ru[i]) / (totalExec[i]);
                tempSum += ru[i];
             }
        }
        sim.averageResourceUtil = tempSum / sim.m;
        for(i=0;i<sim.m;i++){
            tempSum2 =  tempSum2 + ((sim.averageResourceUtil - ru[i])*(sim.averageResourceUtil - ru[i]));
        }
        balanceDev = (Math.sqrt( tempSum2 / sim.m));
        sim.loadBalanceLevel = ( 1 - (balanceDev/sim.m))*100 ;

    }
   //MINMIN with Refinery
    private void schedule_Refinery(Vector<Task> metaSet, int currentTime){

        /*We do not actually delete the task from the meta-set rather mark it as removed*/

        System.out.println(" \t \t Refinery Heuristic ");
        System.out.println("  \t \t First phase ");
        boolean[] isRemoved=new boolean[metaSet.size()];
        double[] ru = new double[sim.m];
        double[] totalExec = new double[sim.m];
        double tempSum = 0;
       // int[][] machineTasks = new int[sim.m][metaSet.size()];
        int machineTasks[] = new int[sim.m];
        double balanceDev = 0;
        double tempSum2 = 0;
        /*Matrix to contain the completion time of each task in the meta-set on each machine.*/
        int c[][]=schedule_MinMinHelper(metaSet);
        int i=0;

        int[] tasksSeries = new int[metaSet.size()];
        int[][] machinesAndTasks = new int[sim.m][metaSet.size()];
        int[] machineSeries = new int[metaSet.size()];

        for(int j=0;j<metaSet.size();j++){
            tasksSeries[j] = -1;
            machineSeries[j] = -1;
            for(int k=0;k<sim.m;k++){
                machinesAndTasks[k][j] = -1;
            }
        }

        int tasksRemoved=0;
        do{
            int minTime=Integer.MAX_VALUE;
            int machine=-1;
            int taskNo=-1;
              /*Find the task in the meta set with the earliest completion time and the machine that obtains it.*/
            for(i=0;i<metaSet.size();i++){
                if(isRemoved[i])continue;
                for(int j=0;j<sim.m;j++){
                    if(c[i][j]< minTime){
                        minTime=c[i][j];
                        machine=j;
                        taskNo=i;
                    }
                }
            }

             Task t=metaSet.elementAt(taskNo);
             ru[machine] += sim.etc[metaSet.get(taskNo).tid][machine];
             sim.mapTask(t, machine);
            /*Mark this task as removed*/
             isRemoved[taskNo]=true;

            /*Update c[][] Matrix for other tasks in the meta-set*/
            for(i=0;i<metaSet.size();i++){
                if(isRemoved[i])continue;
                else{
                    c[i][machine]=sim.mat[machine]+sim.etc[metaSet.get(i).tid][machine];
                }
            }
            out.println("Assigning task "+t.tid+" to machine "+machine+". Completion time = "+t.cTime);
             tasksSeries[tasksRemoved] = taskNo;
             machineSeries[tasksRemoved] = machine;
             machinesAndTasks[machine][machineTasks[machine]] = taskNo;
             machineTasks[machine]++;
             totalExec[machine] = t.cTime;
             tasksRemoved++;

        }while(tasksRemoved!=metaSet.size());
         out.println( " \t \t Second Phase" ) ;
        // sorting and finding highest task based on their completion time
        boolean canBeStopped  = false;
        int count =0;
        while(canBeStopped==false){

            int highestMachine = 0;
            double highestTime = -1;
            for(int m=0;m<sim.m;m++){
                if(totalExec[m] > highestTime){
                    highestMachine = m ;
                    highestTime = totalExec[m];
                }
            }

            int exchangeNumber = 0;
            double highestTime1 = highestTime;
             out.println( "make span time: " + highestTime+ " At machine :" + highestMachine ) ;
             
            int firstTask = -1;
            int secondTask = -1;
            int toMachine = -1;
            int fromIndex = -1;
            int toIndex =-1;
            double execA  =0;
            double execB = 0;

            for(int a=0;a<machineTasks[highestMachine];a++){
                int taskA = machinesAndTasks[highestMachine][a];
                for(int m=0;m<sim.m;m++){
                    if(m == highestMachine) continue;
                    for(int b=0; b<machineTasks[m]; b++){
                        int taskB = machinesAndTasks[m][b];
                        double totalExec1 = totalExec[m] + sim.etc[metaSet.get(taskA).tid][m] - sim.etc[metaSet.get(taskB).tid][m];
                        double totalExec2 = totalExec[highestMachine] - sim.etc[metaSet.get(taskA).tid][highestMachine] + sim.etc[metaSet.get(taskB).tid][highestMachine];
                       // out.println("Result is : " + totalExec1 + " " + totalExec2 + " " + highestTime + " " + highestTime1 );
                        if(totalExec1<highestTime && totalExec2<highestTime && totalExec1<highestTime1 && totalExec2<highestTime1 && totalExec1 >=0 && totalExec2 >=0 ){
                            if(totalExec1>totalExec2) highestTime1 = totalExec1;
                            else highestTime1 = totalExec2;
                            exchangeNumber++;
                            firstTask = taskA;
                            secondTask = taskB;
                            toMachine = m;
                            fromIndex = a;
                            toIndex = b;
                            execA = totalExec2;
                            execB = totalExec1;
                       //     out.println("The exchange has been done and it is : " + taskA + " " + taskB + " " + totalExec1 + " " + totalExec2 + " " + highestTime + " " );
                           // sim.mat[highestMachine] += sim.etc[metaSet.get(taskB).tid][highestMachine] - sim.etc[metaSet.get(taskA).tid][highestMachine];
                            //sim.mat[m] += sim.etc[metaSet.get(taskA).tid][m] - sim.etc[metaSet.get(taskB).tid][m];
                        }
                       // out.println("Result is : " + exchangeNumber);
                    }
                }
            }

            if(exchangeNumber==0){
                canBeStopped = true;
            }
            else if(exchangeNumber!=0){
                System.out.println("new make span time  ="+highestTime1 + " firstTask = "+ firstTask
                            + " secondTask ="+ secondTask+  " toMachine =" + toMachine + " fromIndex " + fromIndex + " toIndex " +  toIndex);
                int a1 = -1;
                int b1 = -1;
                machinesAndTasks[highestMachine][fromIndex] = secondTask ;
                machinesAndTasks[toMachine][toIndex] = firstTask ;
                for(int n =0;n<metaSet.size();n++){
                    if(tasksSeries[n] == firstTask) a1=n;
                }
                for(int n =0;n<metaSet.size();n++){
                    if(tasksSeries[n] == secondTask) b1=n;
                }
                tasksSeries[a1] = secondTask;
                tasksSeries[b1] = firstTask;
                ru[highestMachine] += sim.etc[metaSet.get(secondTask).tid][highestMachine] - sim.etc[metaSet.get(firstTask).tid][highestMachine];
                ru[toMachine] += sim.etc[metaSet.get(firstTask).tid][toMachine] - sim.etc[metaSet.get(secondTask).tid][toMachine];
                totalExec[highestMachine] = execA;
                totalExec[toMachine] = execB;
            }

        }

       // for(int h1=0;h1<metaSet.size();h1++) out.println(machineSeries[h1]);
      //  for(int h1=0;h1<metaSet.size();h1++) out.println(tasksSeries[h1]);

        out.println(" \t Final schedule");
        for(int h1=0;h1<sim.m;h1++) sim.mat[h1]=0;

        for(int n=0;n<metaSet.size();n++){
            int t1 = tasksSeries[n];
            int t2 =  machineSeries[n];
            Task t3=metaSet.elementAt(t1);
            sim.mapTask(t3, t2);
            out.println("Assigning task " + t3.tid  + " to machine " + t2 );
        }


        for(i=0;i<sim.m;i++){
             if(totalExec[i]==0){
                 ru[i] = 0;
             }
             else{
                 ru[i] = (ru[i]) / (totalExec[i]);
                tempSum += ru[i];
             }
        }
        sim.averageResourceUtil = tempSum / sim.m;
        for(i=0;i<sim.m;i++){
            tempSum2 =  tempSum2 + ((sim.averageResourceUtil - ru[i])*(sim.averageResourceUtil - ru[i]));
        }
        balanceDev = (Math.sqrt( tempSum2 / sim.m));
        sim.loadBalanceLevel = ( 1 - (balanceDev/sim.m))*100 ;

    }

// min-min
 private void schedule_MinMin(Vector<Task> metaSet, int currentTime){

        /*We do not actually delete the task from the meta-set rather mark it as removed*/
       System.out.println("\n \t \t Min Min Heuristic\n");
        boolean[] isRemoved=new boolean[metaSet.size()];
        double[] ru = new double[sim.m];
        double[] totalExec = new double[sim.m];
        double tempSum = 0;
        double balanceDev = 0;
        double tempSum2 = 0;

        /*Matrix to contain the completion time of each task in the meta-set on each machine.*/
        int c[][]=schedule_MinMinHelper(metaSet);
        int i=0;

        int tasksRemoved=0;
        do{
            int minTime=Integer.MAX_VALUE;
            int machine=-1;
            int taskNo=-1;
            /*Find the task in the meta set with the earliest completion time and the machine that obtains it.*/
            for(i=0;i<metaSet.size();i++){
                if(isRemoved[i])continue;
                for(int j=0;j<sim.m;j++){
                    if(c[i][j]<minTime){
                        minTime=c[i][j];
                        machine=j;
                        taskNo=i;
                    }
                }
            }
            Task t=metaSet.elementAt(taskNo);
            ru[machine] += sim.etc[metaSet.get(taskNo).tid][machine];
            sim.mapTask(t, machine);

            /*Mark this task as removed*/
            tasksRemoved++;
            isRemoved[taskNo]=true;
            //metaSet.remove(taskNo);

            /*Update c[][] Matrix for other tasks in the meta-set*/
            for(i=0;i<metaSet.size();i++){
                if(isRemoved[i])continue;
                else{
                    c[i][machine]=sim.mat[machine]+sim.etc[metaSet.get(i).tid][machine];
                    
                }
            }
            out.println("Assigning task "+t.tid+" to machine "+machine+". Completion time = "+t.cTime);
            totalExec[machine] = t.cTime;//////

        }while(tasksRemoved!=metaSet.size());
         for(i=0;i<sim.m;i++){
             if(totalExec[i]==0){
                 ru[i] = 0;
             }
             else{
                 ru[i] = (ru[i]) / (totalExec[i]);
                tempSum += ru[i];
             }
        }
        sim.averageResourceUtil = tempSum / sim.m;
        for(i=0;i<sim.m;i++){
            tempSum2 =  tempSum2 + ((sim.averageResourceUtil - ru[i])*(sim.averageResourceUtil - ru[i]));
        }
        balanceDev = (Math.sqrt( tempSum2 / sim.m));
        sim.loadBalanceLevel = ( 1 - (balanceDev/sim.m))*100 ;
    }


    /*This function is a helper of schedule_MinMin() and schedule_MaxMin()*/
    private int[][] schedule_MinMinHelper(Vector<Task> metaSet){
        int c[][]=new int[metaSet.size()][sim.m];
        int i=0;
        for(Iterator it=metaSet.iterator();it.hasNext();){
            Task t=(Task)it.next();
            for(int j=0;j<sim.m;j++){
                c[i][j]=sim.mat[j]+sim.etc[t.tid][j];
            }
            i++;
        }
        return c;
    }

    

    /*This function is a helper of schedule_Sufferage()*/
    private void mapTaskCopy(Task t, int machine, Vector<TaskWrapper> pCopy[], int mat[],int index){
        t.set_eTime(sim.etc[t.tid][machine]);
        t.set_cTime( mat[machine]+sim.etc[t.tid][machine] );

        TaskWrapper tw=new TaskWrapper(index,t);
        pCopy[machine].add(tw);
        mat[machine]=t.cTime;
    }

  
  
  

}
