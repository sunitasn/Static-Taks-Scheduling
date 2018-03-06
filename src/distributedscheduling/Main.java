package distributedscheduling;

import java.text.DecimalFormat;
import java.util.Arrays;
import static java.lang.System.out;
/**
 *
 * @author apurv verma
 */
public class Main {

     public static void main(String...args){

         long t1=System.currentTimeMillis();

        /*Specify the parameters here*/
        int NUM_MACHINES=16;
        int NUM_TASKS=256;
        double ARRIVAL_RATE=19;
        int metaSetSize=15;

        Heuristic h=null;
        TaskHeterogeneity TH=null;
        MachineHeterogeneity MH=null;

        Heuristic HEURISTIC=null;
        TaskHeterogeneity th=TH.HIGH;
        MachineHeterogeneity mh=MH.HIGH;


        int no_of_simulations=1;

        /*Specify the parameters here*/        

        Heuristic[] htype=Heuristic.values();
        
        long avgMakespan=0;
        double avgResource = 0.0;
        double avgLoad = 0.0;
        long sigmaMakespan[]=new long[htype.length];
        double sigmaResource[] = new double[htype.length];
        double sigmaLoad[] = new double[htype.length];
        
        SimulatorEngine se=new SimulatorEngine(NUM_MACHINES, NUM_TASKS, ARRIVAL_RATE, metaSetSize,null,th, mh);

        for(int i=0;i<no_of_simulations;i++){
              
            se.newSimulation(true);
            for(int j=0;j<htype.length;j++){                
                se.setHeuristic(htype[j]);
                //out.println(Arrays.deepToString(se.getEtc()));//////////
                //out.println(Arrays.toString(se.getArrivals()));/////////

                se.simulate();
                
                out.println("\n Makespan ="+se.getMakespan() +"  strategy:"+htype[j].toString());///////////////
                //out.println("Average resource Utilisation  ="+se.averageResourceUtil +"  Load Balancing level: "+se.loadBalanceLevel + "%");
                sigmaMakespan[j]+=se.getMakespan();
                sigmaResource[j] += se.averageResourceUtil;
                sigmaLoad[j] += se.loadBalanceLevel;
                se.newSimulation(false);
            }
         }
 System.out.println();
        for(int j=0;j<htype.length;j++){
            avgMakespan=sigmaMakespan[j]/no_of_simulations;
            avgResource = sigmaResource[j]/no_of_simulations;
            avgLoad = sigmaLoad[j]/no_of_simulations;
            String hName=htype[j].toString();
            String tmp=(String.format("%9s",hName));

            DecimalFormat myFormatter = new DecimalFormat("00000000");
            String output=myFormatter.format(avgMakespan);
           
           // out.println("Avg Resource Utilisation Rate for "+tmp+" heuristic for "+no_of_simulations+ " simulations is =  "+avgResource);
           // out.println("Avg Load Balancing Level for "+tmp+" heuristic for "+no_of_simulations+ " simulations is =  "+avgLoad);
           // out.println("Avg makespan for "+tmp+" heuristic for "+no_of_simulations+ " simulations is =  "+output);
            //out.print(output);
            //out.print("\t");
         }

        long t2=System.currentTimeMillis();
        //out.println("Total time taken in the simulation = "+(t2-t1)/1000+" sec.");
    }
}
