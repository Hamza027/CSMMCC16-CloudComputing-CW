using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
namespace cloudComputing
{
    public class ReduceNFlights
    {
        public string SortingFile = null;
        public string OutputPath = null;
      

        public void Reduce()
        {
            Console.WriteLine("Reducing Flight Information");
            //Create a Dictionary of Airport Codes and Number of Flights
            Dictionary<string, int> FlightsAirport = new Dictionary<string, int>();
            //Now read the file and add each value to the dictionary.
            //All error checking was done before this step
            using (StreamReader reader = new StreamReader(SortingFile))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    string[] Components = line.Split(',');
                    //Add the value to the dictionary
                    FlightsAirport.Add(Components[0], Convert.ToInt16(Components[1]));
                }
            }
            //Now all the values have been added change the order of the dictionary
            var SortedDic = FlightsAirport.OrderByDescending(d => d.Value).ToList();
            //Garbage Collect the unsorted Dictionary
            FlightsAirport = null;
            GC.Collect();
            //Save the results to file
            //Create the Directory
            if (!Directory.Exists(OutputPath + @"/Output"))
            {
                //If not, create it
                Directory.CreateDirectory(OutputPath + @"/Output");
            }
            //See if the file Exists
            string outputFile = OutputPath + @"/Output/T1-FlightOnEachAirport.csv";
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
                foreach (var Air in SortedDic)
                {
                    Console.WriteLine((Air.Key+" | "+ Air.Value));
                    Writer.WriteLine("{0},{1}", Air.Key, Air.Value);
                }
            }
            //Set the chart Interval
            Console.WriteLine("Reduced Flights Per Airport");
        }
    }
}
