package simpledb;

import java.util.*;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 */

public class Catalog {
    
    private ArrayList<Table> _tables;

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    private class Table{
	int _tableID;
	DbFile _file;
	TupleDesc _schema;
	String _name;

	public Table(DbFile file, TupleDesc t, String name){
	    _tableID = file.id();
	    _file = file;
	    _schema = t;
	    _name = name;
	}
	public String getName(){
	    return _name;
	}
	public TupleDesc getSchema(){
	    return _schema;
	}
	public DbFile getData(){
	    return _file;
	}
	public int getID(){
	    return _tableID;
	}
    }
    public Catalog() {

	_tables = new ArrayList<Table>();

     }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.id() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param t the format of tuples that are being added
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     */
    public void addTable(DbFile file, TupleDesc t, String name) {
	Table newTable = new Table(file,t,name);
	this._tables.add(newTable);
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.id() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param t the format of tuples that are being added
     */
    public void addTable(DbFile file, TupleDesc t) {
        addTable(file, t, "");
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) {
	for (int i=0; i<this._tables.size(); i++){
	    if(this._tables.get(i).getName().equals(name))
		return this._tables.get(i).getID();
        }
	throw new NoSuchElementException();
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.id()
     *     function passed to addTable
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
	for (int i=0; i<this._tables.size(); i++){
	    if(this._tables.get(i).getID()==tableid)
		return this._tables.get(i).getSchema();
        }
       	throw new NoSuchElementException();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.id()
     *     function passed to addTable
     */
    public DbFile getDbFile(int tableid) throws NoSuchElementException {
	for (int i=0; i<this._tables.size(); i++){
	    if(this._tables.get(i).getID()==tableid)
		return this._tables.get(i).getData();
        }
        return null;
    }

    /** Delete all tables from the catalog */
    public void clear() {
        this._tables.clear();
    }
}
