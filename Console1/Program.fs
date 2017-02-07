open System
open System.Data
open System.Data.SqlClient

module F =
  let readString() =
    let mutable res = Unchecked.defaultof<string>
    { new DbComb.IReader<_> with
        member x.Consume(objs: obj [], start: int, length: int, value: byref<'a>): bool = 
          let s = objs.[start] :?> string
          value <- s
          true
        member x.Count: int = 1
        member x.Drain(objs: obj [], start: int, length: int, value: byref<'a>): bool =
          false
         }

  let r2s = DbComb.ReaderBuilding.Combine(readString(), readString())

  let readF<'a>() =
    let mutable res = Unchecked.defaultof<'a>
    { new DbComb.IReader<_> with
        member x.Consume(objs: obj [], start: int, length: int, value: byref<'a>): bool = 
          let s = objs.[start] :?> 'a
          value <- s
          true
        member x.Count: int = 1
        member x.Drain(objs: obj [], start: int, length: int, value: byref<'a>): bool =
          false }

  let readPk<'a>() =
    let mutable res = Unchecked.defaultof<'a>
    { new DbComb.IReader<_> with
        member x.Consume(objs: obj [], start: int, length: int, value: byref<'a>): bool = 
          let s = objs.[start] :?> 'a
          value <- s
          true
        member x.Count: int = 1
        member x.Drain(objs: obj [], start: int, length: int, value: byref<'a>): bool =
          false }

  type MyFinal = {
    Id : int
    Name:string
    Price: decimal
    Size: string
    Modified: DateTime
    Sales: SalesOrderDetail[]
  } and SalesOrderDetail = {
    UnitPrice : decimal
    Discount: decimal
    Quantity: int16
    LineTotal : decimal 
  }

  let readFinal() : DbComb.IReader<_> =
    let ary2sod : obj[] -> SalesOrderDetail =
      function 
      | [|:? decimal as unitprice; :? decimal as discount; :? Int16 as q; :? decimal as tot|] ->
        { SalesOrderDetail.Discount = discount
          UnitPrice = unitprice
          Quantity = q
          LineTotal = tot }
      | _ -> failwith "Logic error"
    let leafRdr = DbComb.ReaderBuilding.Leaf(4, ary2sod)

    let mkFinal (os: obj[]) (children:ResizeArray<_>) =
      match os with
      | [|:? int as id; :? string as name; :? decimal as listp; :? string as size; :? DateTime as modified|] ->
        { MyFinal.Id  = id
          Name = name
          Price = listp
          Size = size
          Modified = modified
          Sales = children.ToArray() }
    DbComb.ReaderBuilding.OneToMany(
      headCount=5,
      extractHeadPk= (fun os -> os.[0] :?> int),
      childRdr = leafRdr,
      combine = mkFinal )
    
[<EntryPoint>]
let main argv =
  let serverName = "localhost"
  let connBuilder = new SqlConnectionStringBuilder()
  connBuilder.ApplicationName <- "WpfTest"
  connBuilder.IntegratedSecurity <- true
  connBuilder.DataSource <- serverName
  connBuilder.InitialCatalog <- "AdventureWorks2014"
  let connStr = connBuilder.ConnectionString

  use conn = new SqlConnection(connStr)
  conn.Open()
  use cmd = conn.CreateCommand()
  cmd.CommandText <- "select column_name, table_name from information_schema.columns"
  using (cmd.ExecuteReader()) <| fun rdr ->
    let res = DbComb.R.readTop rdr (F.r2s)
    ()
    //printfn "RES1 %A" (res |> List.ofSeq)

  cmd.CommandText <- """
   SELECT TOP 100 P.ProductID, 
     P.Name, 
     P.ListPrice, 
     P.Size, 
     P.ModifiedDate, 
     SOD.UnitPrice, 
     SOD.UnitPriceDiscount,
     SOD.OrderQty,
     SOD.LineTotal 
    FROM Production.Product P
    CROSS APPLY (
      select top 3 SOD.*
      from Sales.SalesOrderDetail SOD
      where SOD.ProductID = P.ProductID
      order by SOD.LineTotal DESC) SOD 
    WHERE SOD.UnitPrice > 1000 
    ORDER BY ProductID
  """
  using (cmd.ExecuteReader()) <| fun rdr ->
    let res = DbComb.R.readTop rdr (F.readFinal())

    printfn "RES2 %A" (res |> List.ofSeq)

  let _ = Console.ReadLine()
  0 // return an integer exit code
