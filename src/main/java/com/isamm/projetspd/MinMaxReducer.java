/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.isamm.projetspd;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author marwen
 */
public class MinMaxReducer extends Reducer<IntWritable, IntWritable, Text, IntWritable>{
    private IntWritable min_ir=new IntWritable();
    private IntWritable max_ir=new IntWritable();
    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException{
        int min=values.iterator().next().get(),max=min;
        for(IntWritable val: values){
           if(key.get() == 1){
            if(val.get()<min)
                min=val.get();
           }
           if(key.get()==2){
            if(val.get()>max)
                max=val.get();
           }
        }
        if(key.get()==1){
           this.min_ir.set(min);
            Text minText=new Text("min");
            context.write(minText, min_ir);
          }
        if(key.get()==2){
           this.max_ir.set(max);
           System.out.println("(max"+this.max_ir.get()+ "," +this.min_ir.get()+"min)");
           Text maxText=new Text("max");
           context.write(maxText, max_ir);
        }
    }
}
