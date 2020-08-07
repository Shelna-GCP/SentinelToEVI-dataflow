import xlrd
import apache_beam as beam
import apache_beam.transforms.window as window

def readconfig(sheetname):
    newdict = dict()
    loc = "config.xlsx"
    wb = xlrd.open_workbook(loc)
    sheet = wb.sheet_by_name(sheetname)
    for i in range(sheet.nrows):
        newdict[sheet.cell_value(i, 0)] = sheet.cell_value(i, 1)
    print(newdict)
    return newdict


def startSentinelToEVIdataflow(PROJECT_ID,TEST_NAME,BUCKET):
    # Arguments Used by Dataflow
    argv = [
        '--project={}'.format(PROJECT_ID)
        , '--job_name={}'.format(TEST_NAME)
        , '--save_main_session'
        , '--staging_location=gs://{}/staging/'.format(BUCKET)
        , '--temp_location=gs://{}/tmp/'.format(BUCKET)
        , '--runner=DataflowRunner'
        , '--streaming'
        ]

    # Reading from and Writing to a PubSub Topic in Dataflow

    p = beam.Pipeline(argv=argv)
    (p
     | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(topic=None
                                                  , subscription=full_subscription_name
                                                  , id_label=None
                                                  , timestamp_attribute=None
                                                  ).with_output_types(bytes)
     | 'Transformations' >> beam.Map(lambda line: Transform(line))
     | 'WriteStringsToPubSub-EVIReceiver' >> beam.io.WriteToPubSub(topic=full_receiver_topic_name)
     )

    # Windowning in Dataflow

    p = beam.Pipeline(argv=argv)
    K = beam.typehints.TypeVariable('K')
    V = beam.typehints.TypeVariable('V')
    (p
     | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(topic=None
                                                  , subscription=full_subscription_name
                                                  , id_label=None
                                                  , timestamp_attribute=None
                                                  ).with_output_types(bytes)
     | 'Encode' >> beam.Map(lambda line: line.encode("utf-8"))
     # | 'Window' >> beam.WindowInto(window.FixedWindows(5.0))
     | 'Window' >> beam.WindowInto(window.Sessions(60.0))
     | 'GroupBy' >> beam.GroupByKey().with_output_types(beam.typehints.Tuple[K, V])
     )