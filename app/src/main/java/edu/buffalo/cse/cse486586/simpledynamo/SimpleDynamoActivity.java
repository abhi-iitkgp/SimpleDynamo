package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

public class SimpleDynamoActivity extends Activity implements View.OnClickListener
{

    public final Uri mUri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");

    @Override
	protected void onCreate(Bundle savedInstanceState)
    {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
    
		TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        Button b = (Button)findViewById(R.id.testContentProvider);
        b.setOnClickListener(this);
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu)
    {
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	
	public void onStop()
    {
        super.onStop();
	    Log.v("Test", "onStop()");
	}

    @Override
    public void onClick(View v)
    {
        ContentValues cv = new ContentValues();
        String key = "key1";
        String value = "value1";

        cv.put("key", "key1");
        cv.put("value", "value1");

        try
        {
            // insert
            getContentResolver().insert(mUri, cv);

            // query
            Cursor cursor = getContentResolver().query(mUri, null, key, null, null);

            while(cursor.isAfterLast() != true)
            {
                String key_found = cursor.getString(0);
                String value_found = cursor.getString(1);
                Log.v("QUERY_REQUEST_onclick", key_found + " " + value_found);
                cursor.moveToNext();
            }

            // delete
            getContentResolver().delete(mUri, key, null);
        }
        catch(Exception e)
        {

        }
    }
}
