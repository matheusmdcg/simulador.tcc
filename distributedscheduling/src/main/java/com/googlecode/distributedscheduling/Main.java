package com.googlecode.distributedscheduling;

import com.opencsv.CSVWriter;
import com.opencsv.CSVWriterBuilder;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;

import static java.lang.System.out;
import static java.lang.System.setOut;

/**
 * @author apurv verma
 */
public class Main {

    public static void main(String... args) throws IOException {
        boolean byFile = false;
        boolean alwaysGenerateETC = false;

        /*Specify the parameters here*/
        int NUM_MACHINES = 0; //256;
        int NUM_TASKS = 0; //8192;
        int no_of_simulations = 1;

        double ARRIVAL_RATE = 19;
        int metaSetSize = 64;
        String fileName = null;
        String fileNamenew = null;
        String fileNameResults = null;

        Heuristic h = null;
        TaskHeterogeneity TH = null;
        MachineHeterogeneity MH = null;
        Heuristic HEURISTIC = null;
        TaskHeterogeneity th = TH.HIGH;
        MachineHeterogeneity mh = MH.HIGH;



        Scanner scannerUser = new Scanner(System.in);
        System.out.println("Você quer pegar os dados de um arquivo? S/N");
        String saveFile = scannerUser.nextLine();
        if (saveFile.equalsIgnoreCase("S")) {
            byFile = true;
            System.out.println("Qual o nome do arquivo que você quer ler?");
                fileName = "./instancias/"+scannerUser.nextLine();
            try {

                System.out.println("Vou ler o arquivo então");
                Scanner scanner = new Scanner(new FileReader(fileName));
                String line = scanner.nextLine();

                NUM_TASKS = Integer.parseInt(line.split(" ")[0]); //qtd de tarefas
                NUM_MACHINES = Integer.parseInt(line.split(" ")[1]); //qtd de máquinas

                System.out.println("Numero de maquinas: "+ NUM_MACHINES);
                System.out.println("Numero de tarefas "+ NUM_TASKS);

                scanner.close();
                System.out.println("Fechei o arquivo");
            } catch (FileNotFoundException ex) {
                ex.printStackTrace();
            }

        }
        else {
            System.out.println("Vou precisar de alguns dados:");
            System.out.println("Quantas máquinas?");
            NUM_MACHINES = Integer.parseInt(scannerUser.nextLine());

            System.out.println("Quantas tasks?");
            NUM_TASKS = Integer.parseInt(scannerUser.nextLine());

//            System.out.println("Quantas simulações?");
//            no_of_simulations = Integer.parseInt(scannerUser.nextLine());
            no_of_simulations = 1;

            System.out.println("heterogeneidade padrão alta: 3000");
            System.out.println("heterogeneidade padrão baixa: 100");

            System.out.println("Qual heterogeneidade das tarefas? Digite um número inteiro");
            th.setNumericValue(Integer.parseInt(scannerUser.nextLine()));

            System.out.println("Qual heterogeneidade das máquinas?");
            mh.setNumericValue(Integer.parseInt(scannerUser.nextLine()));


            System.out.println("Você quer salvar os dados gerados em um arquivo para utilizar em simulações futuras? S/N");
            if (scannerUser.nextLine().equalsIgnoreCase("S")) {
                System.out.println("Qual o nome do arquivo?");
                fileNamenew =  scannerUser.nextLine();
                fileNamenew = "./instancias/"+fileNamenew;
                File newFile = new File(fileNamenew);
                boolean result;
                try {
                    result = newFile.createNewFile();  //creates a new file
                    if (result)      // test if successfully created a new file
                    {
                        System.out.println("file created " + newFile.getCanonicalPath()); //returns the path string
                    } else {
                        System.out.println("File already exist at location: " + newFile.getCanonicalPath());
                    }
//                    fileScanner = new Scanner(new FileReader(fileName));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            else{
                System.out.println("Já que não vai ser o mesmo arquivo, quer gerar quantas simulações?");
                alwaysGenerateETC = true;
                no_of_simulations = Integer.parseInt(scannerUser.nextLine());
            }

        }

        System.out.println("Qual Nome do arquivo que você quer criar para colocar os resultados?");
            fileNameResults = "./resultados/"+scannerUser.nextLine();
        File resultados = new File(fileNameResults);

        List<String> heuristicas = new ArrayList<String>();
        System.out.println("Você quer executar todas as heuristicas? S/N");
        String todas = scannerUser.nextLine();
        if (todas.equalsIgnoreCase("S")){
            heuristicas.addAll(Arrays.asList("MET", "MCT", "MinMin", "XSufferage", "MinMean", "MaxMin", "MinVar", "OLB", "MinMax"));
        }
        else {
            System.out.println("Qual heuristica você quer executar(por padrão o MET sempre será executado)?");
            String tempRead = scannerUser.nextLine();

            heuristicas.addAll(Arrays.asList("MET", tempRead));
        }
        out.println("Heuristicas:"+heuristicas);


        long t1 = System.currentTimeMillis();
        long tempTemp = t1;
        long tempTemp2 = t1;


        /*Specify the parameters here*/

        double[] sigmaMakespan = new double[heuristicas.size()];
        double[] averageUtilizationList = new double[heuristicas.size()];
        long[] flowTimeList = new long[heuristicas.size()];
        long[] computationTimeList = new long[heuristicas.size()];
        Map<Integer, List<Double>> makespans = new HashMap<>();
        Map<Integer, List<Double>> avgUtilizations = new HashMap<>();
        Map<Integer, List<Long>> flowtimes = new HashMap<>();
        Map<Integer, List<Double>> computationTimes = new HashMap<>();
        double avgMakespan;
        double makespanMET = 0;
        double averageUtilization;
        double averageFlowTime;
        double averageComputationTime;

        metaSetSize = NUM_TASKS;

        SimulatorEngine se;
        if (fileName != null)
            se = new SimulatorEngine(NUM_MACHINES, NUM_TASKS, ARRIVAL_RATE, metaSetSize, null, fileName);
        else if (fileNamenew != null)
            se = new SimulatorEngine(NUM_MACHINES, NUM_TASKS, ARRIVAL_RATE, metaSetSize, null, th, mh, fileNamenew);
        else se = new SimulatorEngine(NUM_MACHINES, NUM_TASKS, ARRIVAL_RATE, metaSetSize, null, th, mh);

        for (int i = 0; i < no_of_simulations; i++) {

            se.newSimulation(alwaysGenerateETC, fileName);
//            se.cleanMatPriorityQueue();

            for (int j = 0; j < heuristicas.size() ; j++) {

                out.println("heuristica:"+heuristicas.get(j));

                se.setHeuristic(heuristicas.get(j));
                se.simulate();

                sigmaMakespan[j] += se.getMakespan();
                makespans.putIfAbsent(j, new ArrayList<>());
                List<Double> makespansForHeuristic = makespans.get(j);
                makespansForHeuristic.add(se.getMakespan());
                makespans.replace(j, makespansForHeuristic);

//                out.println("FlowTime se.mat.sum:"+ Arrays.stream(se.mat).sum());
//                out.println("SumMat calculado separado:" + se.sumMAT());
                averageUtilizationList[j] += se.sumMAT() / (se.makespan * se.m);
                avgUtilizations.putIfAbsent(j, new ArrayList<>());
                List<Double> avgUtilizationsForHeuristic = avgUtilizations.get(j);
                avgUtilizationsForHeuristic.add((double) se.sumMAT() / (double) (se.makespan * se.m));
                avgUtilizations.replace(j, avgUtilizationsForHeuristic);


                flowTimeList[j] += se.sumMAT();
//                out.println("SUMAT = "+flowTimeList[j]);
                flowtimes.putIfAbsent(j, new ArrayList<>());
                List<Long> avgFlowTimesForHeuristic = flowtimes.get(j);
                avgFlowTimesForHeuristic.add((long) se.sumMAT());
                flowtimes.replace(j, avgFlowTimesForHeuristic);

                computationTimeList[j] += se.eng.computationTime;
//                System.out.println(j + " ComputationalTime: " + se.eng.computationTime);
                computationTimes.putIfAbsent(j, new ArrayList<>());
                List<Double> computationTimesForHeuristic = computationTimes.get(j);
                computationTimesForHeuristic.add((double) se.eng.computationTime);
                computationTimes.replace(j, computationTimesForHeuristic);

                se.newSimulation(false, fileName);
                se.cleanMatPriorityQueue();

                //Antes chamava essa função, que não gerava uma nova simulação, só limpava o mat e a fila de prioridade: se.newSimulation(false);


                tempTemp = System.currentTimeMillis();
                out.println("Total time taken in the simulation = " + (tempTemp - tempTemp2) / 1000 + " sec.");
                tempTemp2 = tempTemp;
                out.println("---------------");
            }
        }

        List<Double> avgMakespans = new ArrayList<>();
        List<Double> makespanStandardDeviations = new ArrayList<>();
        List<Double> avgUtilizationList = new ArrayList<>();
        List<Double> utilizationStandardDeviations = new ArrayList<>();
        List<Double> flowTimes = new ArrayList<>();
        List<Double> flowtimeStandardDeviations = new ArrayList<>();
        List<Double> computationTimesList = new ArrayList<>();
        List<Double> computationStandardDeviations = new ArrayList<>();


        BufferedWriter bw = null;
        FileOutputStream fos = null;
        CSVWriter writer = (CSVWriter) new CSVWriterBuilder(new FileWriter(fileNameResults+".csv"))
                .withSeparator(',')
                .build();
        if(no_of_simulations == 1)
            writer.writeNext(("Heuristica#Makespan#FlowTime#matchingProximity#Utilização média das máquinas#Tempo computacional").split("#"));
        else if (no_of_simulations > 1) {
            writer.writeNext(("Heuristica#Makespan médio#Desvio Padrão Makespan#" +
                    "FlowTime médio#Desvio Padrão Flowtime#" +
                    "matchingProximity#"+
                    "Utilização média das máquinas#Desvio Padrão Utilização#"+
                    "Tempo computacional médio#Desvio Padrão Tempo computacional#").split("#"));
        }
        try{
//            fos = new FileOutputStream(resultados);
//            bw = new BufferedWriter(new OutputStreamWriter(fos));
            double avgMakespanMET = 0;
            for (int j = 0; j < heuristicas.size(); j++) {
                if (heuristicas.get(j) == "MET")
                    avgMakespanMET = sigmaMakespan[j] / no_of_simulations;

                avgMakespan = sigmaMakespan[j] / no_of_simulations;
                avgMakespans.add(avgMakespan);
                Double standardDeviation = calculateStandardDeviation(makespans.get(j), avgMakespan);
                makespanStandardDeviations.add(standardDeviation);


                averageUtilization = averageUtilizationList[j] / no_of_simulations;
                avgUtilizationList.add(averageUtilization);
                Double utilizationStandardDeviation = calculateStandardDeviation(avgUtilizations.get(j), averageUtilization);
                utilizationStandardDeviations.add(utilizationStandardDeviation);

                averageFlowTime = (double) flowTimeList[j] / no_of_simulations;
                flowTimes.add(averageFlowTime);
                Double flowtimeStandardDeviation = calculateStandardDeviation(flowtimes.get(j), (long) averageFlowTime);
                flowtimeStandardDeviations.add(flowtimeStandardDeviation);

                averageComputationTime = (double) computationTimeList[j] / no_of_simulations;
                computationTimesList.add(averageComputationTime);
                Double computationStandardDeviation = calculateStandardDeviation(computationTimes.get(j), averageComputationTime);
                computationStandardDeviations.add(computationStandardDeviation);


                String hName = heuristicas.get(j);

                double matchingProximity = (double)avgMakespan / (double)avgMakespanMET;

                String[] entries = {"deu problema na quantidade de simulações"};
                if(no_of_simulations == 1){
                    entries = new String[]{hName,
                    String.valueOf(BigDecimal.valueOf(avgMakespan).setScale(3, RoundingMode.CEILING)),
                    String.valueOf(BigDecimal.valueOf(averageFlowTime).setScale(3, RoundingMode.CEILING)),
                    String.valueOf(BigDecimal.valueOf(matchingProximity).setScale(3, RoundingMode.CEILING)),
                    String.valueOf(BigDecimal.valueOf(averageUtilization).setScale(3, RoundingMode.CEILING)),
                    String.valueOf(BigDecimal.valueOf(averageComputationTime))};
                } else if (no_of_simulations > 1) {
                    entries = new String[]{hName,
                    String.valueOf(BigDecimal.valueOf(avgMakespan).setScale(3, RoundingMode.CEILING)),
                    String.valueOf(BigDecimal.valueOf(standardDeviation*100/avgMakespan).setScale(3, RoundingMode.CEILING))+'%',

                    String.valueOf(BigDecimal.valueOf(averageFlowTime).setScale(3, RoundingMode.CEILING)),
                    String.valueOf(BigDecimal.valueOf(flowtimeStandardDeviation*100/averageFlowTime).setScale(3, RoundingMode.CEILING))+'%',

                    String.valueOf(BigDecimal.valueOf(matchingProximity).setScale(3, RoundingMode.CEILING)),

                    String.valueOf(BigDecimal.valueOf(averageUtilization).setScale(3, RoundingMode.CEILING)),
                    String.valueOf(BigDecimal.valueOf(utilizationStandardDeviation*100/averageUtilization).setScale(3, RoundingMode.CEILING))+'%',

                    String.valueOf(BigDecimal.valueOf(averageComputationTime)),
                    String.valueOf(BigDecimal.valueOf(computationStandardDeviation*100/averageComputationTime).setScale(3, RoundingMode.CEILING))+'%'

                    };
                }

                writer.writeNext(entries);



//                String outputAvg = String.format("%12.8E", (double) avgMakespan);
//                String outputDp = String.format("%12.8E", standardDeviation);
//
//                String outputAvgUt = String.format("%8.8f", averageUtilization);
//                String outputAvgUtDp = String.format("%8.8f", utilizationStandardDeviation);
//
//                String outputAvgFlowTime = String.format("%12.8E", averageFlowTime);
//                String outputAvgFlowTimeDp = String.format("%12.8E", flowtimeStandardDeviation);


//                out.println("\nAvg makespan for " + hName + " heuristic for " + no_of_simulations + " simulations is =  " + avgMakespan);
//                out.println("Makespan Standard Deviation for " + hName + " heuristic for " + no_of_simulations + " simulations is =  " + standardDeviation);
//
//                out.println("Average Utilization for " + hName + " heuristic for " + no_of_simulations + " simulations is =  " + averageUtilization);
//                out.println("Average Utilization Standard Deviation for " + hName + " heuristic for " + no_of_simulations + " simulations is =  " + utilizationStandardDeviation);
//
//                out.println("Average Flowtime for " + hName + " heuristic for " + no_of_simulations + " simulations is =  " + averageFlowTime);
//                out.println("Flowtime Standard Deviation for " + hName + " heuristic for " + no_of_simulations + " simulations is =  " + flowtimeStandardDeviation);
//
//                out.println("Average Computation Time for " + hName + " heuristic for " + no_of_simulations + " simulations is =  " + averageComputationTime + "ms.");
//                out.println("Computation Time Standard Deviation for " + hName + " heuristic for " + no_of_simulations + " simulations is =  " + computationStandardDeviation + "ms.");
            }
            if(bw != null)
                bw.close();
        }catch (IOException err){
            err.printStackTrace();
        }
        writer.close();

        long t2 = System.currentTimeMillis();
//        out.println("\nTotal time taken in the simulation = " + (t2 - t1) / 1000 + " sec.");
//        out.println("Minimum avg makespan = " + htype[avgMakespans.indexOf(Collections.min(avgMakespans))].toString());
//        out.println("Maximum avg makespan = " + htype[avgMakespans.indexOf(Collections.max(avgMakespans))].toString());
//        out.println("Minimum makespan standard deviation = " + htype[makespanStandardDeviations.indexOf(Collections.min(makespanStandardDeviations))].toString());
//        out.println("Maximum makespan standard deviation = " + htype[makespanStandardDeviations.indexOf(Collections.max(makespanStandardDeviations))].toString());
//
//        out.println("Minimum avg utilization = " + htype[avgUtilizationList.indexOf(Collections.min(avgUtilizationList))].toString());
//        out.println("Maximum avg utilization = " + htype[avgUtilizationList.indexOf(Collections.max(avgUtilizationList))].toString());
//
//        out.println("Minimum flowtime = " + htype[flowTimes.indexOf(Collections.min(flowTimes))].toString());
//        out.println("Maximum flowtime = " + htype[flowTimes.indexOf(Collections.max(flowTimes))].toString());
//
//        out.println("Minimum Computation Time = "
//                + htype[computationTimesList.indexOf(Collections.min(computationTimesList))].toString());
//
//        out.println("Maximum Computation Time = "
//                + htype[computationTimesList.indexOf(Collections.max(computationTimesList))].toString());

    }


    private static Double calculateStandardDeviation(List<Long> makespans, long avgMakespan) {

        long differenceSum =
                makespans.stream().mapToLong(makespan -> (long) Math.pow(Math.abs(makespan - avgMakespan), 2)).sum();
        long division = differenceSum / makespans.size();
        return Math.sqrt(division);
    }

    private static Double calculateStandardDeviation(List<Double> makespans, double avgMakespan) {

        double differenceSum =
                makespans.stream().mapToDouble(makespan -> Math.pow(Math.abs(makespan - avgMakespan), 2)).sum();
        double division = differenceSum / makespans.size();
        return Math.sqrt(division);
    }
}
