using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace cloudComputing
{
    public class ReduceLineOSightEFlight
    {
        public ReduceLineOSightEFlight()
        {
        }
        public string outputpath = null;
        public string FlightStart = null;
        public string FlightEnd =  null;
        public string FlightPassengers = null;
        private Dictionary<string, double> FlightDistance = new Dictionary<string, double>();
        private string ErrorText = null;

        public void Reduce()
        {
            Console.WriteLine("Reducing Flight Information");
            //Now reduce the Flight Distance Calculation
            //Create a Dictionary for each flight and start and end lat long
            Dictionary<string, Tuple<double, double>> FlightOri = new Dictionary<string, Tuple<double, double>>();
            Dictionary<string, Tuple<double, double>> FlightDest = new Dictionary<string, Tuple<double, double>>();
            //Load the Flight Start
            using (StreamReader reader = new StreamReader(FlightStart))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    string[] Components = line.Split(',');
                    //Add the value to the dictionary
                    //Check to see if the key is already there
                    if (!FlightOri.ContainsKey(Components[0]))
                    {
                        //Add the lat and long for the start airport
                        FlightOri.Add(Components[0], Tuple.Create(Convert.ToDouble(Components[1]), Convert.ToDouble(Components[2])));
                    }
                }
            }
            //Load the Flight End
            using (StreamReader reader = new StreamReader(FlightEnd))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    string[] Components = line.Split(',');
                    //Add the value to the dictionary
                    //Check to see if the key is already there
                    if (!FlightDest.ContainsKey(Components[0]))
                    {
                        //Add the lat and long for the start airport
                        FlightDest.Add(Components[0], Tuple.Create(Convert.ToDouble(Components[1]), Convert.ToDouble(Components[2])));
                    }
                }
            }
            //Now go through each entry in the start to set the disance keys and calculate Distance
            foreach (var apt in FlightOri)
            {
                //Get the Lat Long Start
                Tuple<double, double> S = apt.Value;
                //Now get the end
                Tuple<double, double> E = FlightDest[apt.Key];
                //Calculate the Distance
                double Dist = Distance(S, E);
                //Add the key, we know these will be unique from last step
                FlightDistance.Add(apt.Key, Dist);
            }
            //Garbage Collect
            FlightOri = null;
            FlightDest = null;
            //Now sort the flights Distance in Descending
            var SortedFlight = FlightDistance.OrderByDescending(d => d.Value).ToList();
            GC.Collect();
            
                foreach (var Flight in SortedFlight)
                {
                    Console.WriteLine("{0},{1}", Flight.Key, Flight.Value);
                    
                }
            
            //Now Calculate the Passenger that's travelled the greatest distance
            Dictionary<string, double> PassengerDistance = new Dictionary<string, double>();
            //Load the file
            using (StreamReader Reader = new StreamReader(FlightPassengers))
            {
                string line;
                double Distance = 0;
                while ((line = Reader.ReadLine()) != null)
                {
                    string[] Components = line.Split(',');
                    //Add the value to the dictionary
                    //Get the Distance from the flight
                    if (FlightDistance.ContainsKey(Components[1]))
                    {
                        //Get the Distance
                        Distance = FlightDistance[Components[1]];
                    }
                    else
                    {
                        //Log the Error
                        ErrorText = ErrorText + "Unable to Find Flight " + Components[0] + Environment.NewLine;
                    }
                    //Check to see if the key is already there
                    if (!PassengerDistance.ContainsKey(Components[0]))
                    {
                        //Add the passenger and Distance
                        PassengerDistance.Add(Components[0], Distance);
                    }
                    else
                    {
                        //Get the old value and add the new value
                        double OldDistance = PassengerDistance[Components[0]];
                        Distance = Distance + OldDistance;
                        PassengerDistance[Components[0]] = Distance;
                    }
                }
            }
            //Do Garbage Collection on the Flight Distance
            FlightDistance = null;
            GC.Collect();
            //Sort the Passenger Distance
            var SortedPassengers = PassengerDistance.OrderByDescending(d => d.Value).ToList();

            foreach (var Passenger in SortedPassengers)
            {
                Console.WriteLine("{0},{1}", Passenger.Key, Passenger.Value);

            }

            var PasHAMile = SortedPassengers[0];

            //Now save Highest Air Travelled Passenger file
            using (StreamWriter Writer = new StreamWriter(outputpath + @"/Output/T4-PassengerWHighestAirMiles.csv"))
            {
                Writer.WriteLine("{0},{1}",PasHAMile.Key,PasHAMile.Value );
            }


            //Write the Error Text
            if (File.Exists(outputpath + @"/3.PassengerDistanceErrors.txt"))
            {
                //Try to delete it
                try
                {
                    File.Delete(outputpath + @"/3.PassengerDistanceErrors.txt");
                }
                catch (IOException)
                {
                    Console.WriteLine("Unable to Delete PassengerDistanceErrors.txt");
                    return;
                }
                catch (UnauthorizedAccessException)
                {
                    Console.WriteLine("Unable to Delete PassengerDistanceErrors.txt");
                    return;
                }
            }
            //Now save the contents to file
            using (StreamWriter Writer = new StreamWriter(outputpath + @"/3.PassengerDistanceErrors.txt"))
            {
                Writer.Write(ErrorText);
            }

            SortedFlight = null;
            SortedPassengers = null;
            GC.Collect();
        }
        private double Rad(double angleIn10thofaDegree)
        {
            // Angle in 10th of a degree
            return (angleIn10thofaDegree * Math.PI) / 1800;
        }
        private double Distance(Tuple<double, double> Origin, Tuple<double, double> Destination)
        {
            //Row 1 latitude Row 2 long
            //Get latitude radians
            var lat = Rad((Destination.Item1 - Origin.Item1));
            //long
            var lng = Rad((Destination.Item2 - Origin.Item2));
            var h1 = Math.Sin(lat / 2) * Math.Sin(lat / 2) +
              Math.Cos(Rad(Origin.Item1)) * Math.Cos(Rad(Destination.Item1)) *
              Math.Sin(lng / 2) * Math.Sin(lng / 2);
            var h2 = 2 * Math.Asin(Math.Min(1, Math.Sqrt(h1)));
            //Times by miles
            return 3960 * h2;
        }
    }
}
