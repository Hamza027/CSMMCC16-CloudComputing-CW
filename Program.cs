using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.IO;


namespace cloudComputing
{
    class MainClass
    {

        private void OpenPassF(ref string pasFile)
        {
            //Open AComp_passenger_data file
            string passFName = @"/Users/itcarelaptops/Projects/cloudComputing/AComp_Passenger_data.csv";
            FileInfo pf = new FileInfo(passFName);

            if (!pf.Name.Equals(""))
            {
                //If a file has been selected, store its name to a variable
                pasFile = Convert.ToString(pf.Name);
            }

        }

        private void OpenAirF(ref string AirFile)
        {
            //Open Airport File
            string AirpFName = @"/Users/itcarelaptops/Projects/cloudComputing/Top30_airports_LatLong.csv";
            FileInfo af = new FileInfo(AirpFName);

            if (!af.Name.Equals(""))
            {
                //If a file has been selected, store its name to a variable
                AirFile = Convert.ToString(af.Name);
            }

        }

        private void GetOutputD(ref string outPath)
        {
            //Get Output Directory
            string outDir = @"/Users/itcarelaptops/Projects/cloudComputing/outputdir";
            FileInfo od = new FileInfo(outDir);

            if (!od.Equals(""))
            {
                //If a folder has been selected, stores its path to a variable
                outPath = Convert.ToString(od);
            }


        }


        public static void Main(string[] args)
        {
            MainClass CSMCC16 = new MainClass();
            string passFile = null;
            string AirFile = null;
            string outPath = null;

            CSMCC16.OpenPassF(ref passFile);
            CSMCC16.OpenAirF(ref AirFile);
            CSMCC16.GetOutputD(ref outPath);

            Console.WriteLine(Environment.NewLine +
                "*-*-*-*-*  Starting Map Reduce *-*-*-*-*"
                + Environment.NewLine);

            Mapper map = new Mapper();

            map.AirFile = AirFile;
            map.PassFile = passFile;
            map.outPath = outPath;

            map.AirportMap();
            map.PassangerMap();


            //Now add Tasks to the dictionary so that it can run on multiple threads.
            Dictionary<string, string[]> MapOutput = new Dictionary<string, string[]>();


            //Objective 1 (A list of all flights from airport and the list of airport codes)
            string[] NFlightFiles = { map.ALatFile, map.FrmAFile };
            MapOutput.Add("NFlight", NFlightFiles);

            //Objective 2 (List of flights with all the data)
            string[] AllFlightInfo = { map.FltPFile, map.FltDpAFile, map.FltDsFile, map.FltDpTFile, map.FltTFile };
            MapOutput.Add("ListFlight", AllFlightInfo);

            //Objective 3 (Number of Passengers on each flight)
            string[] PassengerFlightFiles = { map.FltPFile };
            MapOutput.Add("NPassFlight", PassengerFlightFiles);

            //Objective 4 (Distance For each flight)
            string[] DistanceFlightFiles = { map.ALatFile, map.ALonFile, map.FltDpAFile, map.FltDsFile, map.FltPFile };
            MapOutput.Add("LineOSightEF", DistanceFlightFiles);


            Sorter sort = new Sorter();
            sort.outputpath = map.outPath;

            
            GC.Collect();

            //Now set the parallel threading options
            var Paralleloptions = new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount / 2 };
            Parallel.ForEach(MapOutput, Paralleloptions, MapperOut =>
            {
                //Now run all task files stored in dictionary
                sort.Sort(MapperOut.Key, MapperOut.Value);
                //Garbage collect from each thread and free up memory
                GC.Collect();
                GC.WaitForPendingFinalizers();
            });

            map.Delete_D_Local_Files();
            map = null;
            sort.Delete_D_Local_Files();
            sort = null;
            Console.WriteLine("The End");

        }
    }
}