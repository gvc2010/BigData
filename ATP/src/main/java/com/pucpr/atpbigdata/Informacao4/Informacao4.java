/*
# Grupo 83: 
        - Guilherme Venturini de castro
# Matéria: Fundamentos de Big Data
# Objetivo: É um projeto feito em Java com Maven, 
            criado a partir da IDE NetBeans (versão 8.2) 
            em uma VM.
# Repositório: https://github.com/gvc2010/BigData/tree/main
*/


package com.pucpr.atpbigdata.Informacao4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Informacao4 {
    
    public static class MapperInformacao4 extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException {
            String linha = valor.toString();
            String[] campos = linha.split(";");
            
            if(campos.length == 10) {
                String mercadoria = campos[3];
                int quantidade = 1;

                Text chaveMap = new Text(mercadoria);
                IntWritable valorMap = new IntWritable(quantidade);
                
                context.write(chaveMap, valorMap); 
            }
        }        
    }
    
    public static class ReducerInformacao4 extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text chave, Iterable<IntWritable> valores, Context context) throws IOException, InterruptedException {
            int soma = 0;
                      
            ArrayList<Integer> qtdSoma = new ArrayList();
            
            for(IntWritable valor : valores){
                soma += valor.get();
                qtdSoma.add(soma);
            }
            
            int maiorAtual = Collections.max(qtdSoma);
            
            System.out.println(maiorAtual);
            context.write(chave, new IntWritable(soma));
        }
    }
      public static void main(String[] args) throws Exception {
        System.out.println("Analisando Mercadoria com maior total de peso, de acordo com todas as transações comerciais... ");
        String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
        String arquivoSaida = "/home2/ead2023/SEM1/guilherme.venturini/Desktop/ATP/Informacao4";
        
        if(args.length == 2)
        {
            arquivoEntrada = args[0];
            arquivoSaida = args[1];
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "implementacao4");
        
        job.setJarByClass(Informacao4.class);
        job.setMapperClass(MapperInformacao4.class);
        job.setReducerClass(ReducerInformacao4.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida));
        
        job.waitForCompletion(true); 
    }
}