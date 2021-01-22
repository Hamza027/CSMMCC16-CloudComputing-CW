using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
namespace cloudComputing
{
    public class Sorter
    {
     
        public string outputpath = null;
        public string FlightsAirportFile = null;
        public string FlightsPassengersFile = null;
        
        public string FlightStartLocFile = null;
        public string FlightEndLocFile = null;
        public string PassengerFlights = null;
        private static ReaderWriterLockSlim lock_ = new ReaderWriterLockSlim();
       
        //Function to delete the sort directory and all local files
        public void Delete_D_Local_Files()
        {

            Console.WriteLine("Deleting Local Sort Files");
            string[] files = { FlightStartLocFile, FlightEndLocFile, PassengerFlights, FlightsAirportFile, FlightsPassengersFile };
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
            if (Directory.Exists(outputpath + @"/Sort"))
            {
                //Create if no directory exists
                Directory.Delete(outputpath + @"/Sort");
            }

        }

        public void Sort(string SortType, string[] MapOutput)
        {
            switch (SortType)
            {
                case "NFlight":
                    
                    Console.WriteLine(Environment.NewLine
                        + "*-*-*-*-*  Number Of Flights From Each Airport *-*-*-*-*"
                        + Environment.NewLine);
                    //Calculate the Number of Flights from each airport
                    NFLights(MapOutput);
                    break;
                case "NPassFlight":
                    Console.WriteLine(Environment.NewLine
                        + "*-*-*-*-*  Number Of Passangers On Each Flight *-*-*-*-*"
                        + Environment.NewLine);
                    //Calculate the number of Passengers on each flight
                    NPassFlights(MapOutput);
                    break;
                case "LineOSightEF":
                    Console.WriteLine(Environment.NewLine
                       + "*-*-*-*-*  Line-Of-Sight Of Each Flight *-*-*-*-*"
                       + Environment.NewLine);
                    //Calculate the Flight Distance and total for passengers
                    LineOSightEFs(MapOutput);
                    break;
                case "ListFlight":
                    Console.WriteLine(Environment.NewLine
                        + "*-*-*-*-*  List Of Flights Based On All Information  *-*-*-*-*"
                        + Environment.NewLine);
                    //Get all of the flight info list
                    ListOFlight(MapOutput);
                    break;

            }
        }

        private void ListOFlight(string[] MapOutput)
        {
            //send files to the specific reducer

            ReduceListFlights RLF = new ReduceListFlights();
            RLF.FlightPassengeRLFle = MapOutput[0];
            RLF.FltDpAFile = MapOutput[1];
            RLF.FltDsFile = MapOutput[2];
            RLF.FltDpTFile = MapOutput[3];
            RLF.FltTFile= MapOutput[4];
            RLF.OuputPath = outputpath;
            
            RLF.Reduce();
            
        }
        private void LineOSightEFs(string[] MapOutput)
        {
            Console.WriteLine(" Sorting Flight Distance");
            string ErrorFile = outputpath + @"/FlightsDistanceErrorFile.txt";
            //Create Lat and Long Dictionary
            Dictionary<string, double> AirLat = new Dictionary<string, double>();
            Dictionary<string, double> AirLon = new Dictionary<string, double>();
            //Start by filling the Latitude Dic
            using (StreamReader reader = new StreamReader(MapOutput[0]))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    string[] Components = line.Split(',');
                    AirLat.Add(Components[0], Convert.ToDouble(Components[1]));
                }
            }

            //Fillinf Longitutde Dic
            using (StreamReader reader = new StreamReader(MapOutput[1]))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    string[] Components = line.Split(',');
                    AirLon.Add(Components[0], Convert.ToDouble(Components[1]));
                }
            }

            //Get Starting Lat and long for each flight
            Dictionary<string, Tuple<double, double>> FlightStart = new Dictionary<string, Tuple<double, double>>();
            using (StreamReader reader = new StreamReader(MapOutput[2]))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    string[] Components = line.Split(',');
                    //Get the Lat  for that airport
                    //Check to see if it's in the valid list of airports
                    if (AirLat.ContainsKey(Components[1]))
                    {
                        double lat = AirLat[Components[1]];
                        double lon = AirLon[Components[1]];
                        Tuple<double, double> LatLon = new Tuple<double, double>(lat, lon);
                        if (!FlightStart.ContainsKey(Components[0]))
                        {
                            FlightStart.Add(Components[0], LatLon);
                        }

                    }

                }
            }
            //Write this to file
            //Now save the outputs to a Combiner file
            FlightStartLocFile = outputpath + @"/Sort/FlightStartLoc.csv";

            //Create the Directory
            if (!Directory.Exists(outputpath + @"/Sort"))
            {
                //If not, create it
                Directory.CreateDirectory(outputpath + @"/Sort");
            }
            //Delete the file if it's already there
            if (File.Exists(FlightStartLocFile))
            {
                try
                {
                    File.Delete(FlightStartLocFile);

                }
                catch (IOException)
                {
                    Console.WriteLine("Unable to Delete File " + FlightStartLocFile);
                    return;
                }
                catch (UnauthorizedAccessException)
                {
                    Console.WriteLine("Unable to Delete File " + FlightStartLocFile);
                    return;
                }
            }
            //Now add the records
            using (StreamWriter file = new StreamWriter(FlightStartLocFile))
            {
                foreach (var tup in FlightStart)
                {
                    file.WriteLine("{0},{1},{2}", tup.Key, tup.Value.Item1, tup.Value.Item2);

                }
            }
            //Destroy Flight Start Object
            FlightStart = null;
            //Do Garbage Collection
            GC.Collect();
            GC.WaitForPendingFinalizers();

            //Get Ending Lat and Long for each flight
            Dictionary<string, Tuple<double, double>> FlightEnd = new Dictionary<string, Tuple<double, double>>();
            using (StreamReader reader = new StreamReader(MapOutput[3]))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    string[] Components = line.Split(',');
                    //Get the Lat  for that airport
                    //Check to see if it's in the valid list of airports
                    if (AirLon.ContainsKey(Components[1]))
                    {
                        double lat = AirLat[Components[1]];
                        double lon = AirLon[Components[1]];
                        Tuple<double, double> LatLon = new Tuple<double, double>(lat, lon);
                        if (!FlightEnd.ContainsKey(Components[0]))
                        {
                            FlightEnd.Add(Components[0], LatLon);
                        }
                    }

                }
            }
            //Write this to file
            //Now save the outputs to a Combiner file
            FlightEndLocFile = outputpath + @"/Sort/FlightEndLoc.csv";

            //Delete the file if it's already there
            if (File.Exists(FlightEndLocFile))
            {
                try
                {
                    File.Delete(FlightEndLocFile);

                }
                catch (IOException)
                {
                    Console.WriteLine("Unable to Delete File " + FlightEndLocFile);
                    return;
                }
                catch (UnauthorizedAccessException)
                {
                    Console.WriteLine("Unable to Delete File " + FlightEndLocFile);
                    return;
                }
            }
            //Now add the records
            using (StreamWriter file = new StreamWriter(FlightEndLocFile))
            {
                foreach (var tup in FlightEnd)
                {
                    file.WriteLine("{0},{1},{2}", tup.Key, tup.Value.Item1, tup.Value.Item2);

                }
            }
            //Destroy Flight End Object
            FlightEnd = null;
            //Do Garbage Collection
            GC.Collect();
            GC.WaitForPendingFinalizers();

            //Get the Passenger Flights
            //Just swap the keys around for this mapping
            PassengerFlights = outputpath + @"/Sort/PassengerFlights.csv";

            //Delete the file if it's already there
            if (File.Exists(PassengerFlights))
            {
                try
                {
                    File.Delete(PassengerFlights);

                }
                catch (IOException)
                {
                    Console.WriteLine("Unable to Delete File " + FlightEndLocFile);
                    return;
                }
                catch (UnauthorizedAccessException)
                {
                    Console.WriteLine("Unable to Delete File " + FlightEndLocFile);
                    return;
                }
            }
            List<Tuple<string, string>> PassFlights = new List<Tuple<string, string>>();
            using (StreamReader reader = new StreamReader(MapOutput[4]))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    string[] Components = line.Split(',');
                    PassFlights.Add(Tuple.Create(Components[1], Components[0]));
                }
            }
            //Now write to the new file
            using (StreamWriter file = new StreamWriter(PassengerFlights))
            {
                foreach (var T in PassFlights)
                {
                    file.WriteLine("{0},{1}", T.Item1, T.Item2.ToString());
                }
            }

            //Do Garbage Collection
            PassFlights = null;
            GC.Collect();
            GC.WaitForPendingFinalizers();

            //Now Reduce the Flight Distance
            ReduceLineOSightEFlight RLOSF = new ReduceLineOSightEFlight();
           
            RLOSF.outputpath = outputpath;
            RLOSF.FlightPassengers = PassengerFlights;
            RLOSF.FlightStart = FlightStartLocFile;
            RLOSF.FlightEnd = FlightEndLocFile;
            RLOSF.Reduce();
            //Garbage Collect
            RLOSF = null;
            GC.Collect();

        }
        private void NPassFlights(string[] MapOutput)
        {
            Console.WriteLine("Sorting Flight Passengers ");
            string ErrorFile = outputpath + @"/PassengerFlightsErrorFile.txt";
            //First load all of the unique airport codes into a dictionay
            //create a list of tuples to assign the value, the reducer will then do the calculations later.
            List<Tuple<string, int>> FlightPassengerCount = new List<Tuple<string, int>>();
            //Load the Flight code file which is input 0
            using (StreamReader reader = new StreamReader(MapOutput[0]))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    string[] Components = line.Split(',');
                    //Add the unique airport code with a flight count of 0
                    FlightPassengerCount.Add(Tuple.Create(Components[0], 1));
                }
            }
            //Now write the outputs
            //Now save the outputs to a Combiner file
            FlightsPassengersFile = outputpath + @"/Sort/FlightsPassengers.csv";

            //Create the Directory
            if (!Directory.Exists(outputpath + @"/Sort"))
            {
                //If not, create it
                Directory.CreateDirectory(outputpath + @"/Sort");
            }
            //Delete the file if it's already there
            if (File.Exists(FlightsPassengersFile))
            {
                try
                {
                    File.Delete(FlightsPassengersFile);

                }
                catch (IOException)
                {
                   Console.WriteLine("Unable to Delete File " + FlightsPassengersFile);
                    return;
                }
                catch (UnauthorizedAccessException)
                {
                    Console.WriteLine("Unable to Delete File " + FlightsPassengersFile);
                    return;
                }
            }
            //Now add the records
            using (StreamWriter file = new StreamWriter(FlightsPassengersFile))
            {
                foreach (var tup in FlightPassengerCount)
                {
                    file.WriteLine("{0},{1}", tup.Item1, tup.Item2.ToString());
                }
            }
            //Now call the Reducer
            ReduceNPassOEFlight RNPF = new ReduceNPassOEFlight();
           
            RNPF.OutputPath = outputpath;
            RNPF.SortingFile = FlightsPassengersFile;
            RNPF.Reduce();
            
            //Garbage Collect
            RNPF = null;
            GC.Collect();
            GC.WaitForPendingFinalizers();

        }
        private void NFLights(string[] MapOutput)
        {
            Console.WriteLine( "Sorting Flight Airports ");
            String ErrorFile = outputpath + @"/2.FlightsAirportErrorFile.txt";
            //First load all of the unique airport codes into a dictionay
            //We'll use a dictionary tuple as each code will be unique
            Dictionary<string, int> AirportFlightCount = new Dictionary<string, int>();
            //Load the Airport code file which is input 0
            using (StreamReader reader = new StreamReader(MapOutput[0]))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    string[] Components = line.Split(',');
                    //Add the unique airport code with a flight count of 0
                    AirportFlightCount.Add(Components[0], 0);
                }
            }

            //Now all of the airport codes have been added, we will query the flight logs and sum the flights for each airport
            int FlightCount = 0;
            int ErrorCount = 0;
            //Load the Flights from airport file
            using (StreamReader reader = new StreamReader(MapOutput[1]))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    string[] Components = line.Split(',');
                    //Find the airport code in the Dictionary
                    int Count = 0;
                    //Check to see if it's a valid value

                    if (AirportFlightCount.TryGetValue(Components[0], out Count))
                    {
                        //Add one to that value
                        Count = Count + 1;
                        AirportFlightCount[Components[0]] = Count;
                        FlightCount++;
                    }
                    else
                    {
                        //Add the error to the file
                        String ErrorText = "Invalid Airport Code found during combining " + Components[0];
                        
                        lock_.EnterWriteLock();
                        try
                        {
                            using (var fs = new FileStream(ErrorFile, FileMode.Append, FileAccess.Write))
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
                        ErrorCount++;
                    }

                }
            }
            //Now save the outputs to a Combiner file
            FlightsAirportFile = outputpath + @"/Sort/FlightsAirport.csv";

            //Create the Directory
            if (!Directory.Exists(outputpath + @"/Sort"))
            {
                //If not, create it
                Directory.CreateDirectory(outputpath + @"/Sort");
            }
            //Delete the file if it's already there
            if (File.Exists(FlightsAirportFile))
            {
                try
                {
                    File.Delete(FlightsAirportFile);

                }
                catch (IOException)
                {
                    Console.WriteLine("Unable to Delete File " + FlightsAirportFile);
                    return;
                }
                catch (UnauthorizedAccessException)
                {
                    Console.WriteLine("Unable to Delete File " + FlightsAirportFile);
                    return;
                }
            }

            using (StreamWriter file = new StreamWriter(FlightsAirportFile))
            {
                foreach (var Air in AirportFlightCount)
                {
                    file.WriteLine("{0},{1}", Air.Key, Air.Value.ToString());
                }
            }

            Console.WriteLine(FlightCount.ToString() + " Flights Added");
            Console.WriteLine(ErrorCount.ToString() + " Sorting Errors");
            Console.WriteLine("Sorter File Saved " + FlightsAirportFile);

            //Now Call the Reducer
            Console.WriteLine( "Reducing Flight per Airport");
            ReduceNFlights RF = new ReduceNFlights();
            RF.SortingFile = FlightsAirportFile;
            RF.OutputPath = this.outputpath;
           
            RF.Reduce();
            
            //Garbage Collection
           
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

    }
}
