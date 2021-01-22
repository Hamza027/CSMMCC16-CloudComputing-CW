using System;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;


namespace cloudComputing
{
    public class Mapper
    {
        //Main Passanger and Airport Data files
        public string PassFile = null;
        public string AirFile = null;
        public string outPath = null;


        //Store partitioned data
        public List<string> PassGrpFiles = new List<string>();
        public List<string> AirGrpFiles = new List<string>();

        //Local working variables and files
        public int AKCnt = 0;
        public int PKCnt = 0;
        public int AECnt = 0;
        public int PECnt = 0;
        public string AEFile = null;
        public string PEFile = null;
        public string ALatFile = null;
        public string ALonFile = null;
        public string FltAFile = null;
        public string FltPFile = null;
        public string FltDsFile = null;
        public string FltDpAFile = null;
        public string FltTFile= null;
        public string FltDpTFile = null;
        public string FrmAFile = null;
        //Read Write lock
        private static ReaderWriterLockSlim lock_ = new ReaderWriterLockSlim();



        //Set the  data formats

        //Passenger ID
        private Regex PassID = new Regex("[A-Z]{3}[0-9]{4}[A-Z]{2}[0-9]{1}");

        //Flight ID
        private Regex FltID = new Regex("[A-Z]{3}[0-9]{4}[A-Z]{1}");

        //Airport Code Format
        private Regex AirCode = new Regex("[A-Z]{3}");

        //Departure Time
        private Regex DT = new Regex("[0-9]{10}");

        //Total Flight Time

        private Regex FT = new Regex("[0-9]{1,4}");

        //Latitide and Longtitude
        private Regex Lt_Lng = new Regex(@"[/-]{0,1}[0-9/.]{3,13}");


        // Set the Partitioner to 300 
        public const int Partitioner = 300;


        //Function to delete the Map directory and all local files
        public void Delete_D_Local_Files()
        {

            Console.WriteLine("Deleting Local Map Files");
            string[] files = {ALatFile, ALonFile, FltAFile,FltPFile, FltDsFile, FltDpAFile, FltTFile, FltDpTFile, FrmAFile };
            foreach (string file in files)
            {
                
                //Check if file exists
                if (File.Exists(file))
                {
                    try
                    {
                        //Delete file
                        File.Delete(file);
                    }
                    catch (IOException)
                    {
                        Console.WriteLine("Unable to Delete File " + file);
                        return;
                    }
                    catch (UnauthorizedAccessException)
                    {
                        Console.WriteLine("Unable to Delete File " + file);
                        return;
                    }
                }
               
            }
            if (Directory.Exists(outPath + @"/Map"))
            {
                //If no folder exists, make one
                Directory.Delete(outPath + @"/Map");
            }

        }

        public void AirportMap()
        {
            //Set paths for  local output files
            AEFile = outPath + @"/1.AirportsErrorFile.txt";
            ALatFile = outPath + @"/Map/AirLat.csv";
            ALonFile = outPath + @"/Map/AirLon.csv";


            if (!Directory.Exists(outPath + @"/Map"))
            {
                //If no folder exists, make one
                Directory.CreateDirectory(outPath + @"/Map");
            }


            //Delete Exisiting Files    
            Console.WriteLine("Deleting Existing Airport Mapping Files");
      
            //Create an array of files to loop through for Delete
            string[] files = { AEFile, ALatFile, ALonFile };
            foreach (string file in files)
            {
                //See if  file exists
                if (File.Exists(file))
                {
                    try
                    {
                        //Delete file
                        File.Delete(file);
                    }
                    catch (IOException)
                    {
                        Console.WriteLine("Unable to Delete File " + file);
                        return;
                    }
                    catch (UnauthorizedAccessException)
                    {
                        Console.WriteLine("Unable to Delete File " + file);
                        return;
                    }
                }
            }

            //Open Airports File
            Console.WriteLine("Opening Airport File" + AirFile);

            AirBlocks();

            //Convert airports into required value pairs
            //Run a parallel process to read the lines of the CSV files, one file each thread
            

            var Paralleloptions = new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount / 2 };
            Parallel.ForEach(AirGrpFiles, Paralleloptions, AirChunk =>
            {
                AirpMThread(AirChunk);
            }
            );



            //Set the Log file

            Console.WriteLine(AKCnt + " Airports Added");

            //Delete Temporary Chunking files
            int DelCount = 0;
            foreach (string Airchunk in AirGrpFiles)
            {
                try
                {
                    File.Delete(Airchunk);
                    Console.WriteLine("Deleted File " + Airchunk);
                    //Add that counter to track files deleted
                    DelCount++;
                }
                catch (IOException)
                {
                    Console.WriteLine("Unable to Delete File " + Airchunk);
                    return;
                }
                catch (UnauthorizedAccessException)
                {
                    Console.WriteLine("Unable to Delete File " + Airchunk);
                    return;
                }
            }
            Console.WriteLine(DelCount.ToString() + " Temporary Buffer Files Deleted");


        }

       private void AirBlocks()
        {
            List<string> Lines = new List<string>();
            //Airport File Partitions for multithreading 
            Console.WriteLine("Chunking the Airports File");
            using (StreamReader reader = new StreamReader(AirFile))
            {
                //Read File till end
                string line;
                double BytesRead = 0;
                int FileChunk = 0;
                while ((line = reader.ReadLine()) != null)
                {
                    Lines.Add(line);
                    BytesRead = BytesRead + Encoding.UTF8.GetByteCount(line);
                    //partition data into file based on partitioner limit value
                    if (BytesRead > Partitioner)
                    {
                        //Create a file Split
                        //If we're over the assigned memory write this chunk file and aim for new one
                        string AirChunkFile = outPath + @"/Map/AirChunk_" + FileChunk + ".csv";
                        File.WriteAllLines(AirChunkFile, Lines);
                        //Add the chunk file to list
                        AirGrpFiles.Add(AirChunkFile);
                       
                        FileChunk++;
                        //Clear the memory
                        Lines.Clear();
                        BytesRead = 0;
                    }
                }
                //Update the log
                Console.WriteLine("Airport file split into " + FileChunk.ToString() + " chunks");
            }
        }

        public void AirpMThread(string AirChunk)
        {
            //Run a parallel process to read the lines of the CSV files, one file each thread

            var Paralleloptions = new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount / 2 };
            Parallel.ForEach(File.ReadLines(AirChunk).Select(line => line.Split(',')), Paralleloptions,
                components =>
                {
                    Boolean OK = true;
                    string ErrorText = "";
                    //Check the data format.
                    if (components.Length == 4)
                    {

                        //Check the FAA/IATA Format through Regular Expression
                        if (!AirCode.IsMatch(components[1]))
                        {
                            OK = false;
                            ErrorText = "Invalid Airport Code " + components[1] + " ";
                            AECnt++;
                        }

                        //Check Latitude format

                        if (!Lt_Lng.IsMatch(components[2]))
                        {
                            OK = false;
                            ErrorText = "Invalid Latitude Format " + components[2] + " ";
                            AECnt++;
                        }
                        //Check Longtitude format
                        if (!Lt_Lng.IsMatch(components[3]))
                        {
                            OK = false;
                            ErrorText = "Invalid Longtitude Format " + components[3];
                            AECnt++;
                        }
                        if (OK)
                        {
                            //Write the Mapp Files
                            AKCnt++;
                            //FAA Code & Latitude
                            var line = string.Format("{0},{1}", components[1], components[2]);

                            
                            lock_.EnterWriteLock();
                            try
                            {
                                using (var fs = new FileStream(ALatFile, FileMode.Append, FileAccess.Write))
                                {
                                    using (var fw = new StreamWriter(fs))
                                    {
                                        fw.WriteLine(line);
                                    }

                                }
                            }
                            finally
                            {
                                lock_.ExitWriteLock();
                            }

                            //Airport & Longtitude
                            line = string.Format("{0},{1}", components[1], components[3]);
                            lock_.EnterWriteLock();
                            try
                            {
                                using (var fs = new FileStream(ALonFile, FileMode.Append, FileAccess.Write))
                                {
                                    using (var fw = new StreamWriter(fs))
                                    {
                                        fw.WriteLine(line);
                                    }

                                }
                            }
                            finally
                            {
                                lock_.ExitWriteLock();
                            }
                        }
                        else
                        {
                            //Write Errors to Error file
                            lock_.EnterWriteLock();
                            try
                            {
                                using (var fs = new FileStream(AEFile, FileMode.Append, FileAccess.Write))
                                {
                                    using (var fw = new StreamWriter(fs))
                                    {
                                        fw.WriteLine(ErrorText);
                                    }

                                }
                            }
                            finally
                            {
                                lock_.ExitWriteLock();
                            }
                        }
                    }
                    else
                    {
                        //IF line format Wrong
                        //Write the Errors to file
                        ErrorText = "Invalid Line Format";
                        lock_.EnterWriteLock();
                        try
                        {
                            using (var fs = new FileStream(AEFile, FileMode.Append, FileAccess.Write))
                            {
                                using (var fw = new StreamWriter(fs))
                                {
                                    fw.WriteLine(ErrorText);
                                }

                            }
                        }
                        finally
                        {
                            lock_.ExitWriteLock();
                        }
                    }
                }
                );
        }


        public void PassangerMap()
        {

                
            Console.WriteLine( "Opening Passenger File" + PassFile);

            //Set output file paths
            PEFile = outPath + @"/4.PassengerErrorFile.txt";

            //Check if Mapper Folder exists
            if (!Directory.Exists(outPath + @"/Map"))
            {
                //Create if not exist
                Directory.CreateDirectory(outPath + @"/Map");
            }
            //Set mapper file names
            FltAFile = outPath + @"/Map/FlightAirport.csv";
            FltPFile = outPath + @"/Map/FlightPassenger.csv";
            FltDsFile = outPath + @"/Map/FlightDest.csv";
            FltDpAFile = outPath + @"/Map/FlightDepArpt.csv";
            FltDpTFile = outPath + @"/Map/FlightDepTime.csv";
            FltTFile= outPath + @"/Map/FlightTime.csv";
            FrmAFile = outPath + @"/Map/FromAirport.csv";

            //Create an array of files to loop through for Delete
            Console.WriteLine("Deleting Existing Passenger Mapping Files");
            string[] files = { FltAFile, FltPFile, FltDsFile, FltDpAFile, FltDpTFile, FltTFile, FrmAFile};
            foreach (string file in files)
            {
                //See if file exists
                if (File.Exists(file))
                {
                    try
                    {
                        //Delete the file
                        File.Delete(file);
                    }
                    catch (IOException)
                    {
                        Console.WriteLine("Unable to Delete File " + file);
                        return;
                    }
                    catch (UnauthorizedAccessException)
                    {
                        Console.WriteLine("Unable to Delete File " + file);
                        return;
                    }
                }
            }

            //Now open Passanger  File
            Console.WriteLine( "Opening Passenger File" + PassFile);
            //Now chunk the Passenger file
            ChunkPassengers();

            //Run a  process to read the lines of the chunks files
           
            foreach (string passChunk in PassGrpFiles)
            {
                PassMThread(passChunk);
            }
              



            //Delete the Temporary Chunk files
            int DelCount = 0;
            foreach (string passchunk in PassGrpFiles)
            {
                try
                {
                    File.Delete(passchunk);
                    Console.WriteLine( "Deleted File " + passchunk);
                    //Add that counter to track the removal of chunk files
                    DelCount++;
                }
                catch (IOException)
                {
                    Console.WriteLine("Unable to Delete File " + passchunk);
                    return;
                }
                catch (UnauthorizedAccessException)
                {
                    Console.WriteLine("Unable to Delete File " + passchunk);
                    return;
                }
            }
            //Console Display
            Console.WriteLine( DelCount.ToString() + " Temporary Buffer Files Deleted");
            Console.WriteLine(PKCnt + " Passengers Added");
            Console.WriteLine(PECnt + " Passengers Skipped");
        }

        private void ChunkPassengers()
        {
            //PAssanger File Partitions for multithreading
            List<string> Lines = new List<string>();
            Console.WriteLine("Chunking the Passengers File");
            using (StreamReader reader = new StreamReader(PassFile))
            {
                //Read Lines till the file end
                string line;
                double BytesRead = 0;
                int FileChunk = 0;
                while ((line = reader.ReadLine()) != null)
                {
                    Lines.Add(line);
                    BytesRead = BytesRead + Encoding.UTF8.GetByteCount(line);
                    //Check to see Partitioner limit
                    if (BytesRead > Partitioner)
                    {
                        //Create a file Split
                        //If we're over limit create and write this chunk file and aim for new one
                        string PsnChunkFile = outPath + @"/Map/PassengerChunk_" + FileChunk + ".csv";
                        File.WriteAllLines(PsnChunkFile, Lines);
                        //Add the chunk file to the lists
                        PassGrpFiles.Add(PsnChunkFile);
                       
                        FileChunk++;
                        //Clear the memory
                        Lines.Clear();
                        BytesRead = 0;
                    }
                }
               
                Console.WriteLine("Passenger file split into " + FileChunk.ToString() + " chunks");
            }
        }

        public void PassMThread(string passChunk)
        {
            
            string[] lines = System.IO.File.ReadAllLines(passChunk);

            // Display the file contents by using a foreach loop.
            
            foreach (string line in lines)
            {
               string[] components = line.Split(',');
     
                Boolean OK = true;
                string ErrorText = "";
                //Check file format.
                if (components.Length == 6)
                {
                    //Now check the format of each component using pre-defined regular expressions
                   
                    if (!PassID.IsMatch(components[0]))
                    {
                        OK = false;
                        ErrorText = "Invalid Passenger ID Format " + components[0];
                        PECnt++;
                    }
                    
                    if (!FltID.IsMatch(components[1]))
                    {
                        OK = false;
                        ErrorText = "Invalid Flight ID Format " + components[1];
                        PECnt++;
                    }
                   
                    if (!AirCode.IsMatch(components[2]))
                    {
                        OK = false;
                        ErrorText = "Invalid From Airport Format " + components[2];
                        PECnt++;
                    }
                   
                    if (!AirCode.IsMatch(components[3]))
                    {
                        OK = false;
                        ErrorText = "Invalid To Airport Format " + components[3];
                        PECnt++;
                    }
                    
                    if (!DT.IsMatch(components[4]))
                    {
                        OK = false;
                        ErrorText = "Invalid Departure Time Format " + components[4];
                        PECnt++;
                    }
                    else
                    {
                        
                        //Convert the Unix epoch time to GMT datetime

                        DateTimeOffset dateTimeOffset = DateTimeOffset.FromUnixTimeSeconds(Convert.ToInt64(components[4]));
                        //Exception Handling
                        try
                        {
                            components[4] = dateTimeOffset.ToString("HH:mm:ss");
                        }
                        catch (FormatException)
                        {
                           
                            OK = false;
                            ErrorText = "Invalid Departure Time Format Conversion " + components[4];
                            PECnt++;
                        }
                        catch (ArgumentOutOfRangeException)
                        {
                            
                            OK = false;
                            ErrorText = "Invalid Departure Time Range " + components[4];
                            PECnt++;
                        }
                    }
                    
                    if (!FT.IsMatch(components[5]))
                    {
                        //Invalid Format
                        OK = false;
                        ErrorText = "Invalid Flight Time Format " + components[5];
                        PECnt++;
                    }
                    //IF everything is correct
                    if (OK)
                    {
                        //Write the mapp files
                        PKCnt++;

                        //Flights From Each Airport
                        var lin = string.Format("{0},{1}", components[2], "1");
                        //var lin2 = string.Format("{0},{1}", components[3], "1");


                        lock_.EnterWriteLock();
                        try
                        {
                            using (var fs = new FileStream(FrmAFile, FileMode.Append, FileAccess.Write))
                            {
                                using (var fw = new StreamWriter(fs))
                                {
                                    fw.WriteLine(lin);
                                    //fw.WriteLine(lin2);
                                }

                            }
                        }
                        finally
                        {
                            lock_.ExitWriteLock();
                        }

                        //Passengers on each flight
                        lin = string.Format("{0},{1}", components[1], components[0]);


                        lock_.EnterWriteLock();
                        try
                        {
                            using (var fs = new FileStream(FltPFile, FileMode.Append, FileAccess.Write))
                            {
                                using (var fw = new StreamWriter(fs))
                                {
                                    fw.WriteLine(lin);
                                }

                            }
                        }
                        finally
                        {
                            lock_.ExitWriteLock();
                        }

                        //Flights Departure
                        lin = string.Format("{0},{1}", components[1], components[2]);

                        lock_.EnterWriteLock();
                        try
                        {
                            using (var fs = new FileStream(FltDpAFile, FileMode.Append, FileAccess.Write))
                            {
                                using (var fw = new StreamWriter(fs))
                                {
                                    fw.WriteLine(lin);
                                }

                            }
                        }
                        finally
                        {
                            lock_.ExitWriteLock();
                        }


                        //Flights Departure Time
                        lin = string.Format("{0},{1}", components[1], components[4]);

                        lock_.EnterWriteLock();
                        try
                        {
                            using (var fs = new FileStream(FltDpTFile, FileMode.Append, FileAccess.Write))
                            {
                                using (var fw = new StreamWriter(fs))
                                {
                                    fw.WriteLine(lin);
                                }

                            }
                        }
                        finally
                        {
                            lock_.ExitWriteLock();
                        }

                        //Flights Arrival
                        lin = string.Format("{0},{1}", components[1], components[3]);


                        lock_.EnterWriteLock();
                        try
                        {
                            using (var fs = new FileStream(FltDsFile, FileMode.Append, FileAccess.Write))
                            {
                                using (var fw = new StreamWriter(fs))
                                {
                                    fw.WriteLine(lin);
                                }

                            }
                        }
                        finally
                        {
                            lock_.ExitWriteLock();
                        }

                        //Flights Time
                        lin = string.Format("{0},{1}", components[1], components[5]);


                        lock_.EnterWriteLock();
                        try
                        {
                            using (var fs = new FileStream(FltTFile, FileMode.Append, FileAccess.Write))
                            {
                                using (var fw = new StreamWriter(fs))
                                {
                                    fw.WriteLine(lin);
                                }

                            }
                        }
                        finally
                        {
                            lock_.ExitWriteLock();
                        }
                        PKCnt++;
                    }
                    else
                    {
                        //Write Error to file

                        lock_.EnterWriteLock();
                        try
                        {
                            using (var fs = new FileStream(PEFile, FileMode.Append, FileAccess.Write))
                            {
                                using (var fw = new StreamWriter(fs))
                                {
                                    fw.WriteLine(ErrorText);
                                }

                            }
                        }
                        finally
                        {
                            lock_.ExitWriteLock();
                        }
                        ErrorText = "";
                        OK = true;
                    }
                }
                else
                {
                    //Loop through all of the components to show the error
                    string tError = "";
                    foreach (string s in components)
                    {
                        
                        tError = tError + s + " ";
                    }
                    string err = "";
                    foreach (string e in components)
                    {
                        err = err + e + " ";
                    }
                    ErrorText = "Invalid Passenger Format line " + err;

                    lock_.EnterWriteLock();
                    try
                    {
                        using (var fs = new FileStream(PEFile, FileMode.Append, FileAccess.Write))
                        {
                            using (var fw = new StreamWriter(fs))
                            {
                                fw.WriteLine(err);
                            }

                        }
                    }
                    finally
                    {
                        lock_.ExitWriteLock();
                    }
                    PECnt++;
                }

            } 
               
        }
    }
}
