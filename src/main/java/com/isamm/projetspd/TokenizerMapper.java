/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.isamm.projetspd;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author marwen
 */
public class TokenizerMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
    private IntWritable partition =new IntWritable(0),min =new IntWritable(0),max =new IntWritable(0),nombre=new IntWritable();
    @Override
    public void map(Object key, Text value, Mapper.Context context ) throws IOException,InterruptedException{
        String [] nombres=value.toString().split(" ");
        int longueur = nombres.length, diviseur=diviseur(longueur), min_val=Integer.valueOf(nombres[0]), max_val=min_val, aux;
        for(int i=0;i<longueur;i++){
            if(i % diviseur ==0 ){
                this.partition.set(this.partition.get()+1);
                min_val=Integer.valueOf(nombres[i]);
                max_val=Integer.valueOf(nombres[i]);
            }
            else{
                aux=Integer.valueOf(nombres[i]);
                if(aux< min_val)
                  min_val=aux;
                if(aux> max_val)
                  max_val=aux;
            if((i+1) % diviseur ==0 ){
                this.min.set(min_val);
                this.max.set(max_val);
                this.partition.set(1);
                context.write(this.partition, this.min);
                System.out.println("{"+this.partition.get()+ "," +this.min.get()+"}");
                this.partition.set(2);
                context.write(this.partition, this.max);
                System.out.println("("+this.partition.get()+ "," +this.max.get()+")");
            }
           }
        }
    }
    
    public int diviseur(int longueur)
    {
        if(longueur % 7==0)
        {
            return 7;
        }
        else if(longueur % 5==0)
        {
            return 5;
        }
        else if(longueur % 4==0)
        {
            return 4;
        }
        else if(longueur % 3==0)
        {
            return 3;
        }
        else if(longueur % 2==0)
        {
            return 2;
        }
        else 
        {
            return 1;
        }
    }
}
