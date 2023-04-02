package com.googlecode.distributedscheduling;

import java.util.Vector;
import java.util.Iterator;
import java.util.Random;
import java.util.Random;


import static java.lang.System.out;

/**
 * @author apurv verma
 */
public class SchedulingEngine {

    String heuristic;
    SimulatorEngine sim;
    long computationTime;

    public SchedulingEngine(SimulatorEngine sim, String heuristic){
       this.heuristic=heuristic;
        this.sim=sim;
    }

    private static int randomInRange(int min, int max) {
        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random();
        return r.nextInt(max - min) + min;
    }

    public void schedule(Vector<Task> metaSet,int currentTime){

        long t1 = System.currentTimeMillis();
        /*If any machine has zero assigned tasks then set mat[] for that machine to be the current time.*/
        for(int i=0;i<sim.m;i++){
            if(sim.p[i].isEmpty()){
//                sim.mat[i]=currentTime; //isso daqui é o que deixa cada execução um pouco diferente da outra
                sim.mat[i]=0;
//                if(sim.mat[i] <= 0) {
//                    out.println("sim.mat negativo: " + sim.mat[i]);
//                    out.println("currentTime negativo: " + currentTime);
//                    out.println("+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=");
//                }
            }
        }

        if(heuristic.equals("MET"))
            schedule_MET(metaSet,currentTime);
        else if(heuristic.equals("MCT"))
            schedule_MCT(metaSet,currentTime);
        else if(heuristic.equals("MinMin"))
            schedule_MinMin(metaSet,currentTime);
        else if(heuristic.equals("Sufferage"))
            schedule_Sufferage(metaSet,currentTime);
        else if(heuristic.equals("MinMean"))
            schedule_MinMean(metaSet,currentTime);
        else if(heuristic.equals("MaxMin"))
            schedule_MaxMin(metaSet,currentTime);
        else if(heuristic.equals("MinVar"))
            schedule_MinVar(metaSet,currentTime);
        else if(heuristic.equals("OLB"))
            schedule_olb(metaSet);
//        else if(h==Heuristic.Random)
//            schedule_Random(metaSet);

        long t2 = System.currentTimeMillis();
        this.computationTime = t2-t1;

    }

    private void schedule_MET(Vector<Task> metaSet,int currentTime){
        //O Algoritmo MET designa uma tarefa para o recurso que terá o melhor
        //tempo de execução estimado para essa tarefa, não importando se o recurso está
        //disponível ou não no momento. A motivação deste algoritmo é dar para cada tarefa
        //a melhor máquina, porém isso pode gerar um desbalanceamento nas máquinas e
        //fazer com que uma máquina Ąque sobrecarregada;

        double minExecTime;
        int machine=0;

        for(int i=0;i<metaSet.size();i++){//para cada tarefa
            minExecTime = Integer.MAX_VALUE;
            Task t=metaSet.elementAt(i);
//            out.println("-----------------\nMET\ntask:"+t.tid);
//            out.println("task number:"+i);
            for(int j=0;j<sim.m;j++){ //para cada máquina
                if( sim.etc[t.tid][j] < minExecTime){
                    //sim.etc[t.tid][j] ou etc[tarefa][maquina]
                    //Tempo estimado que a máquina irá levar para processar esta tarefa.
//                    out.println("Machine Number:"+j);
//                    out.println(sim.etc[t.tid][j] + " < " + minExecTime);
                    minExecTime=sim.etc[t.tid][j];
                    machine=j;
                }
            }
//            out.println("Final:");
//            out.println("minExecTime:"+minExecTime);
//            out.println("machine:"+machine);
            sim.mapTask(t, machine);
            //out.println("Adding task "+t.tid+" to machine "+machine+". Completion time = "+t.cTime+" @time "+currentTime);
        }
        //out.println("________Return from schedule_________");///////////////
    }

    private void schedule_MCT(Vector<Task> metaSet,int currentTime){

        double minComplTime;
        int machine=0;

        for(int i=0;i<metaSet.size();i++){
            minComplTime=Integer.MAX_VALUE;
            Task t=metaSet.elementAt(i);
//            out.println("-----------------\nMCT\ntask:"+t.tid);
            for(int j=0;j<sim.m;j++){
                if( sim.etc[t.tid][j] + sim.mat[j] < minComplTime){
                    minComplTime= (sim.etc[t.tid][j] + sim.mat[j]);
                    machine=j;
//                    out.println("********");
//                    out.println("sim.etc[t.tid][j]:"+sim.etc[t.tid][j]);
//                    out.println("sim.mat[j]:"+sim.mat[j]);
                }
            }
//            out.println("Final:");
//            out.println("minComplTime:"+minComplTime);
//            out.println("machine:"+machine);
            sim.mapTask(t, machine);
            //out.println("Adding task "+t.tid+" to machine "+machine+". Completion time = "+t.cTime+" @time "+currentTime);//////
        }
        //out.println("________Return from schedule_________");///////////////
    }

    private void schedule_MinMin(Vector<Task> metaSet, int currentTime){
//        out.println("MinMIn metaset size:"+metaSet.size());
//        out.println("---------------");
//        out.println("Min Min");
        /*We do not actually delete the task from the meta-set rather mark it as removed*/
        boolean[] isRemoved=new boolean[metaSet.size()];

        /*Matrix to contain the completion time of each task in the meta-set on each machine.*/
        double c[][]=schedule_MinMinHelper(metaSet);
        int i=0;

        int tasksRemoved=0;
        do{
            double minTime;
            int machine=-1;
            int taskNo=-1;
            /*Find the task in the meta set with the earliest completion time and the machine that obtains it.*/

            minTime = Integer.MAX_VALUE;
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
//            out.println("Final");
//            out.println("Task:"+taskNo);
//            out.println("Machine:"+machine);
//            out.println("minTime:"+minTime);


            Task t=metaSet.elementAt(taskNo);//pegar no meta-set a task
            sim.mapTask(t, machine);

            /*Mark this task as removed*/
            tasksRemoved++;
            isRemoved[taskNo]=true;
            //metaSet.remove(taskNo);

            /*Update c[][] Matrix for other tasks in the meta-set*/
            for(i=0;i<metaSet.size();i++){
                if(isRemoved[i])
                    continue;
                else{
                    c[i][machine]= (sim.mat[machine]+sim.etc[metaSet.get(i).tid][machine]);
                }
            }

        }while(tasksRemoved!=metaSet.size());//enquanto tiver task
    }


    private void schedule_MaxMin(Vector<Task> metaSet, int currentTime){

        /*We do not actually delete the task from the meta-set rather mark it as removed*/
        boolean[] isRemoved=new boolean[metaSet.size()];

        /*Matrix to contain the completion time of each task in the meta-set on each machine.*/
        double c[][]=schedule_MinMinHelper(metaSet);
        int i=0;

        /*Minimum Completion Time of the ith task in the meta set*/
        double[] minComplTime=new double[metaSet.size()];
        int[] minComplMachine=new int[metaSet.size()];

        int tasksRemoved=0;
        do{
//            c= schedule_MinMinHelper(metaSet);
            double minTime=Integer.MAX_VALUE;
            double maxMinComplTime=Integer.MIN_VALUE;
            int machine=-1;
            int taskNo=-1;
            /*Find the task in the meta set with the earliest completion time and the machine that obtains it.*/
            for(i=0;i<metaSet.size();i++){
                minTime=Integer.MAX_VALUE;
                machine=-1;
                if(isRemoved[i]) continue;
                for(int j=0;j<sim.m;j++){
//                    out.println("J = "+ j);
//                    out.println("I = "+ i);
                    if(c[i][j]<minTime){
                        minTime=c[i][j];
                        machine=j;
//                        out.println("minTime = "+ minTime);
//                        out.println("machine = "+ machine);
                    }
                }
                minComplTime[i]=minTime;
//                out.println("minComplTime["+i+"] = "+ minComplTime[i]);
                minComplMachine[i]=machine;

                if(maxMinComplTime<minComplTime[i]){
                    maxMinComplTime=minComplTime[i];
                    taskNo=i;
                }
            }

            /*Find the task which has the maximum minimum completion time*/
//            double maxMinComplTime=Integer.MIN_VALUE;
//            for(int l=0;l<metaSet.size();l++){
//                if(isRemoved[l]) continue;
//                if(maxMinComplTime<minComplTime[l]){
//                    maxMinComplTime=minComplTime[l];
//                    taskNo=l;
//                }
//            }

            Task t=metaSet.elementAt(taskNo);
            machine=minComplMachine[taskNo];

//            out.println("--------\nFinal");
//            out.println("Task:"+taskNo);
//            out.println("Machine:"+machine);
//            out.println("maxMinComplTime:"+maxMinComplTime);

            sim.mapTask(t, machine);

            /*Mark this task as removed*/
            tasksRemoved++;
            isRemoved[taskNo]=true;
//            out.println("****Task "+ taskNo+" foi removida:"+isRemoved[taskNo] );
            //metaSet.remove(taskNo);

            /*Update c[][] Matrix for other tasks in the meta-set*/
            for(i=0;i<metaSet.size();i++){
                if(isRemoved[i])
                    continue;
                else{
                    c[i][machine]= (sim.mat[machine]+sim.etc[metaSet.get(i).tid][machine]);
                }
            }

        }while(tasksRemoved!=metaSet.size());
    }

    /*This function is a helper of schedule_MinMin() and schedule_MaxMin()*/
    private double[][] schedule_MinMinHelper(Vector<Task> metaSet){
        double c[][]=new double[metaSet.size()][sim.m];
        int i=0;
        for(Iterator it=metaSet.iterator();it.hasNext();){
            Task t=(Task)it.next();
            for(int j=0;j<sim.m;j++){
                c[i][j]= (sim.mat[j]+sim.etc[t.tid][j]);
            }
            i++;
        }
        return c;
    }

    private void schedule_Sufferage(Vector<Task> metaSet, int currentTime) {

        /*We don't directly add the tasks to the p[] matrix of simulator rather add in this copy first*/
        Vector<TaskWrapper> pCopy[]=new Vector[sim.m];

        /*Copy of mat matrix*/
        double[] matCopy=new double[sim.m];
        for(int i=0;i<sim.m;i++){
            matCopy[i]= sim.mat[i];
            /*Also initialize the processors Copy , pCopy[]*/
            pCopy[i]=new Vector<TaskWrapper>(4);
        }

        /*assigned[j] =true tells that machine j has been assigned a task.*/
        boolean assigned[]=new boolean[sim.m];

        /*We do not actually delete the task from the meta-set rather mark it as removed*/
        boolean[] isRemoved=new boolean[metaSet.size()];

        /*Matrix to contain the completion time of each task in the meta-set on each machine.*/
        double c[][]=schedule_MinMinHelper(metaSet);
        int i=0;
        /*Sufferage value of all tasks*/
        double[] sufferage=new double[metaSet.size()];

        int tasksRemoved=0;
        do{
            double minTime1=Integer.MAX_VALUE;
            double minTime2=Integer.MAX_VALUE;
            int machine1=-1;
            int machine2=-1;

            /*For tasks in the meta set,find machine on which it has the earliest and 2nd earliest completion time*/
            for(i=0;i<metaSet.size();i++){
                if(isRemoved[i])continue;
                /*Earliest completion time machine*/
                for(int j=0;j<sim.m;j++){
                    if(c[i][j]<minTime1){
                        minTime1=c[i][j];
                        machine1=j;
                    }
                }
                /*2nd earliest completion time machine*/
                for(int j=0;j<sim.m;j++){
                    if(j!=machine1 && c[i][j]<minTime2 ){
                        minTime2=c[i][j];
                        machine2=j;
                    }
                }
                sufferage[i]=minTime2-minTime1;
                Task t=metaSet.elementAt(i);
                if(!assigned[machine1]){
                    mapTaskCopy(t,machine1,pCopy,matCopy,i);
                    /*Mark this task as removed*/
                    tasksRemoved++;
                    isRemoved[i]=true;
                    //metaSet.remove(taskNo);

                }
                else{
                    for (TaskWrapper tw : pCopy[machine1]) {
                        if (sufferage[tw.getIndex()] < sufferage[i]) {
                            Task task = tw.getTask();
                            int index = tw.getIndex();
                            /*Unassign this task from machine1*/
                            pCopy[machine1].remove(tw);

                            /*Update matCopy[] matrix*/
                            matCopy[machine1] -= sim.etc[task.tid][machine1];

                            /*Add it back to the meta set*/
                            tasksRemoved--;
                            isRemoved[index] = false;

                            /*Assign the current task to the machine*/
                            mapTaskCopy(t, machine1, pCopy, matCopy, i);

                            /*Mark this task as removed*/
                            tasksRemoved++;
                            isRemoved[i] = true;

                        }

                    }
                }
                /*Update c[][] Matrix for other tasks in the meta-set*/
                for(i=0;i<metaSet.size();i++){
                    if(isRemoved[i])continue;
                    else{
                        c[i][machine1]=matCopy[machine1]+sim.etc[metaSet.get(i).tid][machine1];
                    }
                }
            }

        }while(tasksRemoved!=metaSet.size());

        /*Copy matCopy[] and pCopy[] back to original matrices*/
        for(i=0;i<sim.m;i++){
            for(int j=0;j<pCopy[i].size();j++){
                TaskWrapper tbu=pCopy[i].elementAt(j);
                sim.mapTask(tbu.getTask(), i);
            }
        }
        /*By doing this we are preserving the order in which tasks should have been mapped to the machines*/
        System.arraycopy(matCopy, 0, sim.mat, 0, sim.m);
    }


    /*This function is a helper of schedule_Sufferage()*/
    private void mapTaskCopy(Task t, int machine, Vector<TaskWrapper> pCopy[], double mat[],int index){
//        t.set_eTime(sim.etc[t.tid][machine]);
        t.set_cTime( mat[machine]+sim.etc[t.tid][machine] );

        TaskWrapper tw=new TaskWrapper(index,t);
        pCopy[machine].add(tw);
        mat[machine]=t.cTime;
    }

    private void schedule_MinMean(Vector<Task> metaSet, int currentTime) {

        /*We don't directly add the tasks to the p[] matrix of simulator rather add in this copy first*/
        Vector<TaskWrapper> pCopy[]=new Vector[sim.m];

        /*Copy of mat matrix*/
        double[] matCopy=new double[sim.m];
        for(int i=0;i<sim.m;i++){
            matCopy[i]= sim.mat[i];
            /*Also initialize the processors Copy , pCopy[]*/
            pCopy[i]=new Vector<TaskWrapper>(4);
        }

        /*First schedule that tasks according to min-min*/
        schedule_MinMinCopy(metaSet,currentTime,pCopy,matCopy);

        /*Find avg completion time for each machine*/
        long sigmaComplTime=0;
        long avgComplTime=0;
        for(int i=0;i<sim.m;i++)
            sigmaComplTime+=matCopy[i];
        avgComplTime=sigmaComplTime/sim.m;
        int k=0;
        /*Reshufffle tasks from machines which have higher completion time than average to lower compl time machines*/
        for(int i=0;i<sim.m;i++){
            if(matCopy[i]<=avgComplTime)continue;
            k=0;
            while(k+1<=pCopy[i].size()){
                TaskWrapper tw=pCopy[i].elementAt(k);
                Task t=tw.getTask();
                /*Remap this task to another machine with completion time less than average completion time
                 such that the difference of the new completion time of the machine and the average comple-
                 -tion time becomes the minimum.
                 This is analogous to best-fit algorithm
                */
                int delta=Integer.MIN_VALUE;
                int machine=i;
                for(int j=0;j<sim.m;j++){
                    if(j==i || matCopy[j]>=avgComplTime)continue;
                    if(( matCopy[j]+sim.etc[t.tid][j] < avgComplTime) && Math.abs( matCopy[j]+sim.etc[t.tid][j] - avgComplTime) > delta){
                        delta= (int) Math.abs( matCopy[j]+sim.etc[t.tid][j] - avgComplTime);
                        machine=j;
                    }
                }
                /*Map the task to the new machine*/
                if(machine!=i){
                    pCopy[i].remove(tw);
                    matCopy[i]-=sim.etc[t.tid][i];
                    mapTaskCopy(t,machine,pCopy,matCopy,tw.getIndex());

                    /*Note that the new avg completion time may be different from the old one*/
                    //sigmaComplTime-=sim.etc[t.tid][i];
                    //sigmaComplTime+=sim.etc[t.tid][machine];
                    //avgComplTime=sigmaComplTime/sim.m;
                    /*Not included because it increases makespan slightly*/
                }
                k++;
            }
        }
        /*Copy matCopy[] and pCopy[] back to original matrices*/
        for(int i=0;i<sim.m;i++){
            for(int j=0;j<pCopy[i].size();j++){
                TaskWrapper tbu=pCopy[i].elementAt(j);
                sim.mapTask(tbu.getTask(), i);
            }
        }
        /*By doing this we are preserving the order in which tasks should have been mapped to the machines*/
        System.arraycopy(matCopy, 0, sim.mat, 0, sim.m);
    }


    private void schedule_MinMinCopy(Vector<Task> metaSet, int currentTime, Vector<TaskWrapper>[] pCopy, double[] matCopy){

        /*We do not actually delete the task from the meta-set rather mark it as removed*/
        boolean[] isRemoved=new boolean[metaSet.size()];

        /*Matrix to contain the completion time of each task in the meta-set on each machine.*/
        double c[][]=schedule_MinMinCopyHelper(metaSet,matCopy);
        int i=0;

        int tasksRemoved=0;
        do{
            double minTime=Integer.MAX_VALUE;
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
            this.mapTaskCopy(t,machine,pCopy,matCopy,taskNo);

            /*Mark this task as removed*/
            tasksRemoved++;
            isRemoved[taskNo]=true;
            //metaSet.remove(taskNo);

            /*Update c[][] Matrix for other tasks in the meta-set*/
            for(i=0;i<metaSet.size();i++){
                if(isRemoved[i])continue;
                else{
                    c[i][machine]=matCopy[machine]+sim.etc[metaSet.get(i).tid][machine];
                }
            }

        }while(tasksRemoved!=metaSet.size());
    }

    /*This function is a helper of schedule_MinMin()*/
    private double[][] schedule_MinMinCopyHelper(Vector<Task> metaSet, double[] matCopy){
        double c[][]=new double[metaSet.size()][sim.m];
        int i=0;
        for(Iterator it=metaSet.iterator();it.hasNext();){
            Task t=(Task)it.next();
            for(int j=0;j<sim.m;j++){
                c[i][j]=matCopy[j]+sim.etc[t.tid][j];
            }
            i++;
        }
        return c;
    }

    private void schedule_MinVar(Vector<Task> metaSet, int currentTime) {
        /*We don't directly add the tasks to the p[] matrix of simulator rather add in this copy first*/
        Vector<TaskWrapper> pCopy[]=new Vector[sim.m];

        /*Copy of mat matrix*/
        double[] matCopy=new double[sim.m];
        for(int i=0;i<sim.m;i++){
            matCopy[i]= sim.mat[i];
            /*Also initialize the processors Copy , pCopy[]*/
            pCopy[i]=new Vector<TaskWrapper>(4);
        }

        /*First schedule that tasks according to min-min*/
        schedule_MinMinCopy(metaSet,currentTime,pCopy,matCopy);

        /*Find avg completion time for each machine*/
        long sigmaComplTime=0;
        long avgComplTime=0;
        for(int i=0;i<sim.m;i++)
            sigmaComplTime+=matCopy[i];
        avgComplTime=sigmaComplTime/sim.m;


        int k=0;
        /*Reshuffle tasks so that the variance decreases*/
        for(int i=0;i<sim.m;i++){
            if(matCopy[i]<=avgComplTime)continue;
            k=0;
            while(k+1<=pCopy[i].size()){
                TaskWrapper tw=pCopy[i].elementAt(k);
                Task t=tw.getTask();
                int deltaVar=0;
                int minDeltaVar=Integer.MAX_VALUE;

                long newSigmaComplTime=sigmaComplTime;
                long newAvgComplTime=avgComplTime;

                int machine=i;
                int delta=Integer.MIN_VALUE;
                for(int j=0;j<sim.m;j++){
                    if(j==i || matCopy[j]>=avgComplTime)continue;
                    deltaVar=0;

                    newSigmaComplTime-=sim.etc[t.tid][i];
                    newSigmaComplTime+=sim.etc[t.tid][j];
                    newAvgComplTime=newSigmaComplTime/sim.m;

                    deltaVar-=(int) Math.pow((matCopy[i] - avgComplTime),2);
                    deltaVar+=(int) Math.pow((matCopy[i]-sim.etc[t.tid][i]-newAvgComplTime),2);
                    deltaVar-=(int) Math.pow((matCopy[j]-avgComplTime),2);
                    deltaVar+=(int) Math.pow((matCopy[j]+sim.etc[t.tid][j]-newAvgComplTime),2);

                    if(( matCopy[j]+sim.etc[t.tid][j] < avgComplTime) && Math.abs( matCopy[j]+sim.etc[t.tid][j] - avgComplTime) > delta && deltaVar<0 && deltaVar<minDeltaVar){
                        minDeltaVar=deltaVar;
                        delta= (int) Math.abs( matCopy[j]+sim.etc[t.tid][j] - avgComplTime);
                        machine=j;
                    }

                    newSigmaComplTime=sigmaComplTime;
                    newAvgComplTime=avgComplTime;

                }
                /*Map the task to the new machine*/
                if(machine!=i){
                    pCopy[i].remove(tw);
                    matCopy[i]-=sim.etc[t.tid][i];
                    mapTaskCopy(t,machine,pCopy,matCopy,tw.getIndex());
                    /*Note that the new avg completion time may be different from the old one*/
                    sigmaComplTime-=sim.etc[t.tid][i];
                    sigmaComplTime+=sim.etc[t.tid][machine];
                    avgComplTime=sigmaComplTime/sim.m;

                }
                k++;
            }
        }
        /*Copy matCopy[] and pCopy[] back to original matrices*/
        for(int i=0;i<sim.m;i++){
            for(int j=0;j<pCopy[i].size();j++){
                TaskWrapper tbu=pCopy[i].elementAt(j);
                sim.mapTask(tbu.getTask(), i);
            }
        }
        /*By doing this we are preserving the order in which tasks should have been mapped to the machines*/
        System.arraycopy(matCopy, 0, sim.mat, 0, sim.m);
    }


    private void schedule_olb(Vector<Task> metaSet) {

        for (int i = 0; i < metaSet.size(); i++) {
            Task t = metaSet.elementAt(i);
            double minMatTime = Integer.MAX_VALUE;
            int machine = 0;
            for (int j = 0; j < sim.m; j++) {
                if (sim.mat[j] < minMatTime) {
                    minMatTime = sim.mat[j];
                    machine = j;
                }
            }
            sim.mapTask(t, machine);
            //out.println("Adding task "+t.tid+" to machine "+machine+". Completion time = "+t.cTime+" @time "+currentTime);//////
        }
        //out.println("________Return from schedule_________");///////////////
    }

    private void schedule_Random(Vector<Task> metaSet) {

        int machine=0;

        for(int i=0;i<metaSet.size();i++){//para cada tarefa
            Task t=metaSet.elementAt(i);
            machine = randomInRange(0, sim.m);
            sim.mapTask(t, machine);
            //out.println("Adding task "+t.tid+" to machine "+machine+". Completion time = "+t.cTime+" @time "+currentTime);
        }
    }


}
