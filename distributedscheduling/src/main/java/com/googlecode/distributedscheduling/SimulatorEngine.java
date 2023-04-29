package com.googlecode.distributedscheduling;

import java.util.Random;
import java.util.Vector;
import java.util.Comparator;
import java.util.PriorityQueue;
import static java.lang.System.out;


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
    public double etc[][];

    /*Machine availability time, the time at which machine i finishes all previously assigned tasks.*/
    public double mat[];

    public SchedulingEngine eng;

    TaskHeterogeneity TH;
    MachineHeterogeneity MH;
    
    /*For calculating avg completion time*/
    long sigma;

    /*For calculating makespan*/
    double makespan;
    //contrutor generico
    public SimulatorEngine(int NUM_MACHINES, int NUM_TASKS, double ARRIVAL_RATE, int metaSetSize,String HEURISTIC){

        sigma=0;
        makespan=0;

        n=NUM_TASKS;
        S=metaSetSize;
        m=NUM_MACHINES;
        lambda=ARRIVAL_RATE;
        comparator=new TaskComparator();
        p=new PriorityQueue[m];
        eng=new SchedulingEngine(this,HEURISTIC);

        for(int i=0;i<p.length;i++)
            p[i]=new PriorityQueue<Task>(5,comparator);

        mat=new double[m];// Este é o tempo estimado em que a máquina irá Ąnalisar o processamento das tarefas que estão designadas para ela

    }

    //construtor para usar parametros do usuário
    public SimulatorEngine(int NUM_MACHINES, int NUM_TASKS, double ARRIVAL_RATE, int metaSetSize,String HEURISTIC, TaskHeterogeneity th, MachineHeterogeneity mh){

        this(NUM_MACHINES, NUM_TASKS, ARRIVAL_RATE, metaSetSize, HEURISTIC);
        TH = th;
        MH = mh;
        generateRandom(null);
    }

    //construtor para usar parametros do usuário + um arquivo para adicionar os valores aleatórios gerados
    public SimulatorEngine(int NUM_MACHINES, int NUM_TASKS, double ARRIVAL_RATE, int metaSetSize,String HEURISTIC, TaskHeterogeneity th, MachineHeterogeneity mh, String fileNamenew){

        this(NUM_MACHINES, NUM_TASKS, ARRIVAL_RATE, metaSetSize, HEURISTIC);
        TH = th;
        MH = mh;
//        System.out.println("Nome do novo arquivo com as simulações:"+fileNamenew);
        generateRandom(fileNamenew);
    }

    //construtor para usar parametros do arquivo
    public SimulatorEngine(int NUM_MACHINES, int NUM_TASKS, double ARRIVAL_RATE, int metaSetSize,String HEURISTIC, String fileScanner){
        this(NUM_MACHINES, NUM_TASKS, ARRIVAL_RATE, metaSetSize, HEURISTIC);
        generateByFile(fileScanner);
    }

//    private void generateRandoms(){
//        arrivals=new ArrivalGenerator(n,lambda).getArrival();
//        etc=new ETCGenerator(m,n,TH,MH).getETC();//Este é o tempo estimado que as máquinas irão levar para processar cada tarefa.
//    }

    private void generateByFile(String fileScanner){
        arrivals=new ArrivalGenerator(n,lambda).getArrival();
        ETCGenerator etcGeneratorByFile = new ETCGenerator(m, n);
        etcGeneratorByFile.generateETCbyFile(fileScanner);
        etc = etcGeneratorByFile.getETC();


//        etc=new ETCGenerator(m,n).getETC(fileScanner);//Este é o tempo estimado que as máquinas irão levar para processar cada tarefa.
    }

    private void generateRandom(String fileName){
        arrivals=new ArrivalGenerator(n,lambda).getArrival();
        ETCGenerator etcGenerator = new ETCGenerator(m, n, TH, MH);
        etcGenerator.generateETCRandom(fileName);
        etc = etcGenerator.getETC();

//        etc=new ETCGenerator(m,n).getETC(fileScanner);//Este é o tempo estimado que as máquinas irão levar para processar cada tarefa.
    }

    public void newSimulation(boolean alwaysGenerateETC, String fileName){
        makespan=0;
        sigma=0;
        if(alwaysGenerateETC)
            if(fileName == null){
//                out.println("File name Null, vou gerar random de novo");
                generateRandom(null);
            }


        for(int i=0;i<m;i++){
            mat[i]=0;
            p[i].clear();
        }
    }

    public void cleanMatPriorityQueue(){
        makespan=0;
        sigma=0;
        for(int i=0;i<m;i++){
            mat[i]=0;
            p[i].clear();
        }
    }

    public double sumMAT(){
        double sumMat = 0;
        for(int i=0;i<m;i++){
            sumMat = sumMat + mat[i];
//            out.println("SUMMAT="+sumMat);
//            if(mat[i] <= 0)
//                out.println("mat negativo: "+mat[i]);
//            if(sumMat <= 0) {
//                out.println("sumMat negativo: " + sumMat);
//                out.println("mat talvez negativo: "+mat[i]);
//                out.println("================================");
//            }
        }
//        out.println("*********\n");

        return sumMat;
    }

    public void setHeuristic(String h){
        this.eng.heuristic=h;
    }

    public double getMakespan() {
        return makespan;
    }

    public int[] getArrivals() {
        return arrivals;
    }

    public double[][] getEtc() {
        return etc;
    }

    public void mapTask(Task t, int machine){
        double random = new Random().nextDouble();
//        t.set_eTime(etc[t.tid][machine]);
//        t.set_eTime((etc[t.tid][machine]-10) + (int)(random*((etc[t.tid][machine]+10)-(etc[t.tid][machine]-10))));

//        if(mat[machine] <= 0)
//            out.println("mat[machine]"+ mat[machine]);
//        out.println("etc[t.tid][machine]"+ etc[t.tid][machine]);
        t.set_cTime(mat[machine] + etc[t.tid][machine]);
//        double percent = etc[t.tid][machine]/15;
//        t.set_cTime( mat[machine] + (etc[t.tid][machine]-percent + random*((etc[t.tid][machine]+percent)-(etc[t.tid][machine]-percent))));
        //o tempo estimado não é o tempo real que a máquina levará
        p[machine].offer(t);
        mat[machine]=t.get_cTime();
    }

    


    public void simulate(){
        /*tick represents the current time*/
        int tick=0;

        Vector<Task> metaSet=new Vector<Task>(S);

        int i1=0;
        int i2=S;/*Meta-task set size*/
//        out.println("Tamanho do metaset:"+S);

        /*Initialization*/
        /*Add the first S tasks to the meta set and schedule them*/
        for(int i=i1;i<i2;i++){
            Task t = new Task(arrivals[i],i);
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
                    p[i].poll();
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
            result= p[i].isEmpty();
        return result;
    }

    private double max(double a,double b){
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
