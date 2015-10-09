import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
public class fcm {
//第一轮mapper和reducer用来创建随机矩阵，可以实现并行化
public static class RandomMartixMapper extends Mapper<LongWritable,Text,Text,Text>
{
	private Text newkey=new Text();
	private Text newvalue=new Text();
	private StringBuilder sb=new StringBuilder();
	
	  Random ran=new Random(100);
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//value的值为样本点，将这个值作为key传给reduce，然后将随机矩阵的列作为值传给reduce
		      double sum;
		 	  int m = 0;
		 	  int c = 0;
			  m= context.getConfiguration().getInt("m", m);
			  c= context.getConfiguration().getInt("c", c);
			  double martix[]=new double[c];
			  double array[]=new double[c];
			  System.out.println("m="+m+" c="+c);
			  for(int j=0;j<c;j++)
			  {
				  array[j]=ran.nextDouble();
				  System.out.println("array[j]="+array[j]);
			  }
			  sum=0;
			  for(int j=0;j<c;j++)
			  {
				  sum=sum+array[j];
			  }
			  for(int j=0;j<c;j++)
			  {
				  martix[j]=array[j]/sum;
				  sb.append(Math.pow(martix[j],m)+" ");
			  }
			  //把value作为新的key
		//	  System.out.println(value+"       "+sb.toString());
			  newkey=value;
			  newvalue.set(sb.toString());
			  sb=new StringBuilder();
			  context.write(newkey, newvalue);
	//	super.map(key, value, context);
	}
}
//处理随机矩阵生成的reducer
public static class RandomMartixReducer extends Reducer<Text,Text,Text,Text>
{
	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1,Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		for(Text value:arg1)
		{
			System.out.println(arg0+"     "+value);
			arg2.write(arg0, value);
		}
	}
}

/*
第二轮mapreduce主要的任务是生成聚类向量,并行化的思想是主要依赖于上一步的结果，map阶段输出形式为
向量序号和xi+Uij形式
*/
 public static class ClusterCenterPointMapper extends Mapper<Text,Text,IntWritable,Text>
 {
	@Override
	protected void map(Text key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	//	super.map(key, value, context);
	//	System.out.println("key="+key);
	//	System.out.println("valuy="+value);
		String[] values=value.toString().split(" ");
		IntWritable mapkey=new IntWritable();
		Text mapvalue=new Text();
		StringBuilder sb=new StringBuilder();
		for(int i=0;i<values.length;i++)
		{
			mapkey.set(i);
			sb.append(key.toString());
			sb.append(",");
			sb.append(values[i]);
			mapvalue.set(sb.toString());
		//	sb=null;
			sb=new StringBuilder();
			context.write(mapkey, mapvalue);
		}
	}
 }
 public static class ClusterCenterPointReducer extends Reducer<IntWritable,Text,IntWritable,Text>
 {

	@Override
	protected void reduce(IntWritable arg0, Iterable<Text> arg1,Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	//System.out.println("key="+arg0);
		String fenzi=null;
		double fenmu=0;
		Text reducevalue=new Text();
		for(Text value:arg1)
		{
			String[] array=value.toString().split(",");
			double m=Double.parseDouble(array[1]);
			fenmu=fenmu+m;
			String[] points=array[0].split(" ");
			String[] temps=null;
			StringBuilder sb=new StringBuilder();
			if(fenzi==null)
			{
				 temps=new String[points.length];
				 for(int i=0;i<temps.length;i++)
				 {
					 temps[i]="0";
				 }
			}
			else
			{
				temps=fenzi.split(" ");
			}
			for(int i=0;i<points.length;i++)
			{
				points[i]=(Double.parseDouble(temps[i])+Double.parseDouble(points[i]) * m)+"";
			}
			
			for(int i=0;i<points.length;i++)
			{
				sb.append(points[i]+" ");
			}
			fenzi=sb.toString().substring(0,sb.length()-1);
			sb=null;
		//	System.out.println(value.toString());
		}
		String[] res=fenzi.split(" ");
	//	System.out.println("fenmu="+fenmu);
		for(int i=0;i<res.length;i++)
		{
		//	System.out.println("res="+res[i]);
			res[i]=Double.parseDouble(res[i])/fenmu+"";
		}
		StringBuilder sb=new StringBuilder();
		for(int i=0;i<res.length;i++)
		{
		//	System.out.println("res="+res[i]);
			sb.append(res[i]+" ");
		}
		fenzi=sb.toString().substring(0,sb.length()-1);
		reducevalue.set(fenzi);
		arg2.write(arg0, reducevalue);
	}
 }
 //聚类中心存放的形式为0  聚类中心这样的形式，接下来计算价值函数。
 public static class JMapper extends Mapper<Text,Text,Text,DoubleWritable>
 {
    ArrayList<String> clist=new ArrayList<String>();
    IntWritable seqkey=new IntWritable();
    Text seqvalue=new Text();
    Text mapkey=new Text("1");
    DoubleWritable mapvalue=new DoubleWritable();
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	//	System.out.println("in setup");
		FileSystem fs=FileSystem.get(context.getConfiguration());
		SequenceFile.Reader reader=new SequenceFile.Reader(fs, new Path("/home/novas/clusterpointoutput/part-r-00000"), context.getConfiguration());
	   while(reader.next(seqkey, seqvalue))
	   {
	//	   System.out.println(seqvalue.toString());
		   clist.add(seqvalue.toString());
	   }
	}
	public double getDistance(String a1,String a2)
	{
		double res=0;
		String[] temp1=a1.split(" ");
		String[] temp2=a2.split(" ");
		double c1=0;
		double c2=0;
		for(int i=0;i<temp1.length;i++)
		{
			c1=Double.parseDouble(temp1[i]);
			c2=Double.parseDouble(temp2[i]);
		//	System.out.println("c1="+c1+"c2="+c2);
			res=res+(c1-c2)*(c1-c2);
		//	System.out.println(res);
		}
		return Math.sqrt(res);
	}
	@Override
	protected void map(Text key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.map(key, value, context);
	   //System.out.println("in map");
		String[] array=value.toString().split(" ");
		for(int i=0;i<clist.size();i++)
		{
			String s=clist.get(i);
		//	System.out.println("距离点为"+s+"   "+key.toString());
			double res=getDistance(s,key.toString());
		//	System.out.println("距离为"+res);
			mapvalue.set(res*Double.parseDouble(array[i]));
			context.write(mapkey, mapvalue);
		}
	}
 }
 
 public static class JReducer extends Reducer<Text,DoubleWritable,IntWritable,DoubleWritable>
 {
	 double res = 0;
	@Override
	protected void reduce(Text arg0, Iterable<DoubleWritable> arg1,Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.reduce(arg0, arg1, arg2);
	//	System.out.println("in 价值函数"+arg0.toString());
		for(DoubleWritable value : arg1)
		{
			res=res+value.get();
		}
		arg2.write(new IntWritable(1),new DoubleWritable(res));
	}
 }
 
 //重新计算隶属矩阵
 public static class rebuildMapper extends Mapper < Text , Text , Text , DoubleWritable >
 {
	 //在setup的时候读取之前得到的聚类中心点；
	    ArrayList<String> clist=new ArrayList<String>();
	    IntWritable seqkey=new IntWritable();
	    Text seqvalue=new Text();
	    DoubleWritable mapvalue=new DoubleWritable();
		@Override
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		//	System.out.println("in setup");
			FileSystem fs=FileSystem.get(context.getConfiguration());
			SequenceFile.Reader reader=new SequenceFile.Reader(fs, new Path("/home/novas/clusterpointoutput/part-r-00000"), context.getConfiguration());
		   while(reader.next(seqkey, seqvalue))
		   {
			 //  System.out.println(seqvalue.toString());
			   clist.add(seqvalue.toString());
		   }
		}
		public double getDistance(String a1,String a2)
		{
			double res=0;
			String[] temp1=a1.split(" ");
			String[] temp2=a2.split(" ");
			double c1=0;
			double c2=0;
			for(int i=0;i<temp1.length;i++)
			{
				c1=Double.parseDouble(temp1[i]);
				c2=Double.parseDouble(temp2[i]);
			//	System.out.println("c1="+c1+"c2="+c2);
				res=res+(c1-c2)*(c1-c2);
			//	System.out.println(res);
			}
			return Math.sqrt(res);
		}
		@Override
		protected void map(Text key, Text value,
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//super.map(key, value, context);
		   //System.out.println("in map");
			String[] array=value.toString().split(" ");
			for(int i=0;i<clist.size();i++)
			{
				String s=clist.get(i);
			//	System.out.println("距离点为"+s+"   "+key.toString());
				double res=getDistance(s,key.toString());
		//		System.out.println("距离为"+res);
				mapvalue.set(res);
				context.write(key, mapvalue);
			}
		}
 }
 
 public static class rebuildReducer extends Reducer < Text , DoubleWritable , Text , Text >
 {

	 ArrayList<DoubleWritable> list=new ArrayList<DoubleWritable>();
	 Text reducevalue = new Text();
	@Override
	protected void reduce(Text arg0, Iterable<DoubleWritable> arg1,Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		 double sum=0;
		 StringBuilder sb = new StringBuilder();
		 int m = 0;
		m= arg2.getConfiguration().getInt("m", m);
		System.out.println("m="+m);
		for ( DoubleWritable value : arg1 )
		{
			System.out.println("value="+value);
			sum = sum + Math.pow((1.0 / value . get ( )),2.0/(m-1)) ;
			list . add ( new DoubleWritable(value.get())) ;
		}
		for ( int i = 0 ; i < list . size();i ++)
		{
			double l= Math.pow((list.get(i).get()),2.0/(m-1)) ;
			System.out.println(list.get(i).get()+"  "+l);
			sb.append((sum/l)+" ");
		}
		list.clear();
		reducevalue .set ( sb.toString() ) ;
		System.out.println("in rebuild"+arg0+"     "+reducevalue);
		arg2.write(arg0, reducevalue);
	}
 }
  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
  {
	  long atime=System.currentTimeMillis();
	  double  J = Double.MAX_VALUE;//J表示价值函数的值
	  double flag = 5;//自己定义的阈值
	  int c=3;
	  int m=2;
	  Configuration conf = new Configuration ( ) ;
	  conf . setInt ( "c" , c ) ;
	  conf . setInt ( "m", m ) ;
	  FileSystem fs = FileSystem.get ( conf ) ;
	  FileStatus[] fstatus = fs.listStatus ( new Path ("/home/novas") ) ;
	  /*
	   * 配置数据存储相关路径
	   */
	  String deletePath = null ;
	  String readPath = null ;//这个参数的含义是用来交替改变读取位置的
	   readPath = "/home/novas/randommartixoutput" ;
	   deletePath = "/home/novas/randommartixoutput-temp" ;
	   String dataStorePath = "/home/novas/data" ;
	   String clusterpointPath = "/home/novas/clusterpointoutput" ;
	   String JPath = "/home/novas/joutput" ;
	   String resultFileName = "/part-r-00000" ;
	  for ( int i = 0 ; i < fstatus.length ; i++ )
	  {
		//  System.out.println(fstatus[i].getPath());
		  if ( !fstatus[i].getPath ( ).toString ( ).contains ( dataStorePath ) )
		  {
			  fs.delete  ( fstatus[i].getPath ( ) ) ;
		  } 
	  }
	  Job job = new Job ( conf ) ;
	 //设定一些常量参数
 	 job.setMapperClass ( RandomMartixMapper.class ) ;
 	 job.setReducerClass ( RandomMartixReducer.class ) ;
 	 job.setOutputKeyClass ( Text.class);
 	 job.setOutputValueClass ( Text.class);
 	 job.setOutputFormatClass ( SequenceFileOutputFormat.class ) ;
 	 FileInputFormat.addInputPath ( job , new Path ( dataStorePath ) ) ;
 	 SequenceFileOutputFormat.setOutputPath ( job ,  new  Path ( readPath ) ) ;
 	 job.waitForCompletion ( true ) ;
 	 int count = 0 ;
	
 	 while ( J > flag && count < 3 )
 	 {
 		 count++;
 		 Job clusterJob = new Job ( conf ) ;
 		 fs.delete ( new Path ( clusterpointPath ) ) ;
 	 	 clusterJob.setInputFormatClass ( SequenceFileInputFormat.class ) ;
 	 	 clusterJob.setMapperClass ( ClusterCenterPointMapper.class ) ;
 	 	 clusterJob.setReducerClass ( ClusterCenterPointReducer.class ) ;
 	 	 clusterJob.setOutputKeyClass ( IntWritable.class ) ;
 	 	 clusterJob.setOutputValueClass ( Text.class ) ;
 	 	 clusterJob.setOutputFormatClass ( SequenceFileOutputFormat.class ) ;
 	 	 SequenceFileInputFormat.addInputPath ( clusterJob , new Path ( readPath ) ) ;
 	     SequenceFileOutputFormat.setOutputPath ( clusterJob , new Path ( clusterpointPath ) ) ;
 	 	 clusterJob.waitForCompletion ( true ) ;
 	 	 
 	 	 Job Jjob = new Job ( conf ) ;
 	 	 fs.delete ( new Path ( JPath ) ) ;
 	    Jjob.setMapOutputKeyClass ( Text.class ) ;
 	    Jjob.setMapOutputValueClass ( DoubleWritable.class ) ;
 	    Jjob.setOutputKeyClass ( IntWritable.class ) ;
 	    Jjob.setOutputValueClass ( DoubleWritable.class ) ;
 	    Jjob.setMapperClass ( JMapper.class ) ;
 	    Jjob.setReducerClass ( JReducer.class ) ;
 	    Jjob.setInputFormatClass ( SequenceFileInputFormat.class ) ;
 	    Jjob.setOutputFormatClass ( SequenceFileOutputFormat.class ) ;
 	    SequenceFileInputFormat.addInputPath ( Jjob , new Path ( readPath ) ) ;
 	    SequenceFileOutputFormat.setOutputPath ( Jjob , new Path ( JPath ) ) ;
 	   Jjob.waitForCompletion ( true ) ;
 	    
 	   SequenceFile.Reader reader = new SequenceFile.Reader ( fs , new Path ( JPath + resultFileName ) , conf ) ;
 	   IntWritable tempkey = new IntWritable ( ) ;
 	   DoubleWritable tempvalue = new DoubleWritable ( ) ;
 	   while ( reader.next ( tempkey , tempvalue ) )
 	   {
 		   J = tempvalue .get();
 		   System.out.println("当前价值函数为"+J );
 	   }
 	   if ( J > flag )
 	   {
 		   Job rebuildmartixJob = new Job ( conf ) ;
 		   rebuildmartixJob . setMapOutputKeyClass ( Text.class ) ;
 		   rebuildmartixJob . setMapOutputValueClass ( DoubleWritable.class ) ;
 	       rebuildmartixJob. setMapperClass ( rebuildMapper.class ) ;
 	       rebuildmartixJob. setReducerClass( rebuildReducer.class ) ;
 		   rebuildmartixJob. setOutputKeyClass ( Text.class ) ;
 		   rebuildmartixJob. setOutputValueClass ( Text.class ) ;
 		   rebuildmartixJob. setInputFormatClass ( SequenceFileInputFormat.class ) ;
 	       rebuildmartixJob. setOutputFormatClass ( SequenceFileOutputFormat.class ) ;
 	       SequenceFileInputFormat. addInputPath ( rebuildmartixJob , new Path ( readPath ) ) ;
 	       SequenceFileOutputFormat . setOutputPath ( rebuildmartixJob , new  Path ( deletePath ) ) ;
           rebuildmartixJob.waitForCompletion ( true ) ;
           fs.delete ( new Path ( readPath ) ) ;
           fs.rename ( new Path ( deletePath ) , new Path ( readPath ) ) ;
 	   }
 	 }
 	 long btime=System.currentTimeMillis();
 	 System.out.println("时间为"+(btime-atime));
 	   SequenceFile.Reader reader=new SequenceFile.Reader(fs, new Path(readPath+resultFileName), conf);
	   Text tempkey=new Text();
	   Text tempvalue=new Text();
	   while ( reader.next ( tempkey , tempvalue ) )
	   {
		  System.out.println(tempkey.toString()+"  "+tempvalue.toString());
	   }
  }
}
