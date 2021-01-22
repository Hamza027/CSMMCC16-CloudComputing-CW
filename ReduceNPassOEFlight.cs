using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace cloudComputing
{
    public class ReduceNPassOEFlight
    {
      
        public string SortingFile = null;
        public string OutputPath = null;
        
        public void Reduce()
        {
            Console.WriteLine("Reducing Flight Information");
            //Create a Dictionary For the Flights
            Dictionary<string, int> FlightsPassengers = new Dictionary<string, int>();
            //Loop through all of the rows in the file
            using (StreamReader reader = new StreamReader(SortingFile))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    string[] Components = line.Split(',');
                    //Add the value to the dictionary
                    //Check to see if the key is already there
                    if (!FlightsPassengers.ContainsKey(Components[0]))
                    {
                        //Add it
                        FlightsPassengers.Add(Components[0], Convert.ToInt16(Components[1]));
                    }
                    else
                    {
                        //If the key is already there get the Value and add 1
                        int count = 0;
                        count = FlightsPassengers[Components[0]];
                        count = count + 1;
                        //Now save the new Value back
                        FlightsPassengers[Components[0]] = count;
                    }
                }
            }
            //Now Sort according to flights with largest number
            var SortedDic = FlightsPassengers.OrderByDescending(d => d.Value).ToList();
            //Garbage Collect the old Dictionry
            FlightsPassengers = null;
            GC.Collect();

            //Now save the results to file and display them
            //Create the Directory
            if (!Directory.Exists(OutputPath + @"/Output"))
            {
                //If not, create it
                Directory.CreateDirectory(OutputPath + @"/Output");
            }
            //See if the file Exists
            string outputFile = OutputPath + @"/Output/T3-PassengersOnEachFlight.csv";
            // Delete the File if it's there
            if (File.Exists(outputFile))
            {
                try
                {
                    File.Delete(outputFile);
                }
                catch (IOException)
                {
                    Console.WriteLine("Unable to Save File " + outputFile);
                    return;
                }
                catch (UnauthorizedAccessException)
                {
                    Console.WriteLine("Unable to Save File " + outputFile);
                    return;
                }
            }
            //Now save the File and pass the results to the Results Window
            using (StreamWriter Writer = new StreamWriter(outputFile))
            {
                foreach (var flt in SortedDic)
                {
                    Console.WriteLine("{0},{1}", flt.Key, flt.Value);
                    Writer.WriteLine("{0},{1}", flt.Key, flt.Value);
                }
            }
           
            Console.WriteLine("Reduced Passengers Per Flight");
        }
    }
}
