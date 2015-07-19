package hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		/*String tmp = "hello you,hello me";
		Pattern compile = Pattern.compile("[,]");
		Matcher matcher = compile.matcher(tmp);
		String line = matcher.replaceAll("~");
		System.out.println(line);
		String replace = tmp.replace(',', '~');
		System.out.println(replace);*/
		/*String tmp = "hello	you	hello	me";
		StringTokenizer word = new StringTokenizer(tmp);
		while (word.hasMoreTokens()) {
			String s = word.nextToken();
			System.out.println(s);
		}*/
		
		/*System.out.println("please input a num");
		Scanner scanner = new Scanner(System.in);
		int num = scanner.nextInt();
		System.out.println(num);*/
		
		/*if (1 != 2) {
			System.err.println("abnormal");
			System.exit(2);
		}*/
		
		/*List<String> ls = new ArrayList<String>();
		ls.add("hello");
		ls.add("world");
		for (String string : ls) {
			System.out.println(string);
		}
		if (!ls.contains("hel")) {
			System.out.println(1);
		}*/
		
		/*List<String> ls = new ArrayList<String>();
		FileInputStream in = new FileInputStream(new File("C:\\Users\\martin\\Desktop\\stopword.txt"));
		InputStreamReader inputStreamReader = new InputStreamReader(in);
		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
		String line;
		while ((line = bufferedReader.readLine())!= null) {
			ls.add(line);
		}
		bufferedReader.close();
		if (ls.contains("and")) {
			System.out.println(1);
		}*/
		
		//System.out.println(Math.floor(-4.5));
		
		int x=30,y=45;
		x=y++ + x++;
		y=++y + ++x;
		System.out.printf("%d,%d",x,y);

	}

}
