package edu.buffalo.cse.cse486586.simpledht;

import android.app.Activity;
import android.os.Bundle;
import android.text.method.ScrollingMovementMethod;
import android.view.Menu;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.Button;
import android.widget.TextView;

public class SimpleDhtActivity extends Activity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);
        
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));
        
        Button successorButton = (Button) findViewById(R.id.getSuccessor);
        successorButton.setOnClickListener(new OnClickListener() {
			@Override
			public void onClick(View v) {
				TextView tt = (TextView) findViewById(R.id.successor);
				tt.setText("S - " + SimpleDhtProvider.mySuccessor +
						   " , P - " + SimpleDhtProvider.myPredecessor);
			}
		});
        
        findViewById(R.id.button1).setOnClickListener(
        		new OnLDumpClickListener(tv, getContentResolver()));
        
        findViewById(R.id.button2).setOnClickListener(
        		new OnGDumpClickListener(tv, getContentResolver()));
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }
}