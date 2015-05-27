package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;


public class Storage extends SQLiteOpenHelper
{
    static String DB_NAME = "KeyValue.db";

    public Storage(Context context)
    {
        super(context, DB_NAME, null, 1);
    }

    @Override
    public void onCreate(SQLiteDatabase db)
    {
        db.execSQL("CREATE TABLE UserData(key TEXT, value TEXT, version INTEGER)");
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion)
    {
        db.execSQL("DELETE TABLE IF EXISTS UserData");
        onCreate(db);
    }

    public void insert(String key, String value)
    {
        int version = 0;
        SQLiteDatabase db = this.getWritableDatabase();
        ContentValues cv = new ContentValues();
        cv.put("key", key);
        cv.put("value", value);

        // checking if that key already exists
        Cursor cursor = db.rawQuery("SELECT * FROM UserData WHERE key = '" + key + "'", null);
        if(cursor.getCount() > 0)
        {
            cursor.moveToFirst();
            int current_version = cursor.getInt(2);
            version = current_version + 1;
            cv.put("version", version);
            delete(key);
        }

        db.insert("UserData", null, cv);
    }

    public void delete(String key)
    {
        SQLiteDatabase db = this.getWritableDatabase();
        db.delete("UserData", "key = ?", new String[] {key});
    }
}