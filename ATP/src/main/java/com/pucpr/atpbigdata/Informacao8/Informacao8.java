/*
# Grupo 83: 
        - Guilherme Venturini de castro
# Matéria: Fundamentos de Big Data
# Objetivo: É um projeto feito em Java com Maven, 
            criado a partir da IDE NetBeans (versão 8.2) 
            em uma VM.
# Repositório: https://github.com/gvc2010/BigData/tree/main
*/

package com.pucpr.atpbigdata.Informacao8;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Informacao8 {
    
    public static class MapperInformacao8 extends Mapper<Object, Text, Text, LongWritable> {        
        @Override
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException 
        {
            String linha = valor.toString();
            String[] campos = linha.split(";");
            LongWritable valorMap = new LongWritable(0);

            if(campos.length == 10) 
            {
                String mercadoria = campos[3];
                String peso = campos[6];
                String ano = campos[1];

                Text chaveMap = new Text(mercadoria + " >>> " + ano);            
            
                try 
                {
                    valorMap = new LongWritable(Long.parseLong(peso));
                } 
                
                catch (NumberFormatException err) {
                } 
                finally {
                }
                
                context.write(chaveMap, valorMap);
            }
        }
    }   
    
    public static class ReducerInformacao8 extends Reducer<Text, LongWritable, Text, LongWritable> {
    
            public void reduce(Text chave, Iterable<LongWritable> valores, Context context) throws IOException, InterruptedException {
                int soma = 0;
                
                for(LongWritable valor : valores){
                    soma += valor.get();
                    
                }
                LongWritable result = new LongWritable(soma);
                context.write(chave, result);
            }
    }
    
    public static void main(String[] args) throws Exception {
        System.out.println("Analisando Mercadoria com maior total de peso, de acordo com todas as transações comerciais, separadas por ano.... ");
        String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
        String arquivoSaida = "/home2/ead2023/SEM1/guilherme.venturini/Desktop/ATP/Informacao8";
        
        if(args.length == 2){
            arquivoEntrada = args[0];
            arquivoSaida = args[1];
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "implementacao8");
        
        job.setJarByClass(Informacao8.class);
        job.setMapperClass(MapperInformacao8.class);
        job.setReducerClass(ReducerInformacao8.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida));
        
        job.waitForCompletion(true);
 
    }
}