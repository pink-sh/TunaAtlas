# Get the wkt from a/several area name(s) (AreaFilter) - typically set by the user - that is/are in the database with a Postgis geometry type
.fun_get_wkt_or_geom_from_name_area<-function(AreaFilter,con,area_tableName,area_areaColumnName,area_geomColumnName,AreaFilter_geometry_type){

  if(AreaFilter_geometry_type=="geom"){
    postgis_geomtype_function<-NULL
  } else if (AreaFilter_geometry_type=="wkt"){
    postgis_geomtype_function<-"ST_AsText"
  }

  filter=gsub(",", "','", AreaFilter)
  filter=paste("('",filter,"')",sep="")
  query_area<-paste("SELECT ",postgis_geomtype_function,"(ST_Union(",area_geomColumnName,")) as geom_filter from ",area_tableName," where ",area_areaColumnName," IN ",filter,sep="")
  AreaFilter_vquery <- dbGetQuery(con, query_area)
  AreaFilter_vquery<-AreaFilter_vquery$geom_filter

  if (AreaFilter_geometry_type=="wkt"){
    AreaFilter_vquery<-paste("'",AreaFilter_vquery,"'",sep="")
  }

  return(AreaFilter_vquery)
}



# Filter a dimension (i.e. column name) with an information given by the user
.where_clause_function<-function(column_name,filter){

  if (!is.null(filter)){
    filter=gsub(",", "','", filter)
    filter=paste("('",filter,"')",sep="")
    query_where_clause<-paste(" AND ",column_name," IN ",filter,sep="")
  }

  if (is.null(filter)){
    query_where_clause<-NULL
  }

  return(query_where_clause)

}



.CheckNullVariable<-function(variable){
  if (variable=="NULL" | variable=="Automatic choice") {
    variable<-NULL
  }
  return(variable)
}

## When using a table from the Tuna Atlas, get the columns and table names to use as input of the function PrepareQueryOnTable, depending on the parameters set by the user.
.Sardara_GetColumnsAndTableNamesForQuery<-function(DataSource,
                                                  CatchTransformationLevel,
                                                  CodeListSource,
                                                  AreaFilter,
                                                  variable_to_group_by,
                                                  TimeStart,
                                                  TimeEnd,
                                                  TimeResolution,
                                                  SchooltypeFilter,
                                                  SpeciesFilter,
                                                  SpeciesResolution,
                                                  GearFilter,
                                                  GearResolution,
                                                  FlagFilter,
                                                  FlagResolution){


  # determination of the area type
  if ( length(grep("IOTC", AreaFilter))>0  ||  length(grep("ICCAT", AreaFilter))>0   ||  length(grep("IATTC", AreaFilter))>0   ||  length(grep("WCPFC", AreaFilter))>0  ){
    AreaFilter_type<-"rfmo"
  } else {
    AreaFilter_type<-"other"
  }


  #calculation of the time frame (TimeEnd - TimeStart)

  if (is.null(TimeResolution)){
    TimeStart<-as.Date(TimeStart)
    TimeEnd<-as.Date(TimeEnd)

    time_frame<-TimeEnd-TimeStart
  }


  # SELECTION OF THE RIGHT TABLE

  # If DataSource is set, we use the table set by the user
  if (!is.null(DataSource)){

    if (DataSource=='Total catches') {
      dbTable<-'tunaatlas.total_catches_ird_labels'
    }
    if (DataSource=='Catch-and-efforts'){
      if (CatchTransformationLevel=="IRD" || is.null(CatchTransformationLevel)) {
        dbTable<-'tunaatlas.catches_ird_rf1_labels'
      } else {  # else = CatchTransformationLevel="raw"
        dbTable<-'tunaatlas.catches_ird_raw_labels'
      }
    }
  }

  # If DataSource is not set, we set it the following way:
  # If TimeResolution is not set:
  # - if the time frame is superior to 2 year (730 days) AND if the area is an RFMO or several rfmos, we use tunaatlas.total_catches_ird_labels
  # - in the other cases, we use tunaatlas.catches_ird_rf1_labels
  # If TimeResolution is set:
  # - If it is set to month or quarter or semester OR if it is set to year or decade AND the area is a WKT, we use tunaatlas.catches_ird_rf1_labels
  # - in the other cases, we use tunaatlas.total_catches_ird_labels

  if (is.null(DataSource)){

    if (is.null(TimeResolution)){

      if (time_frame>=733 & AreaFilter_type=="rfmo" ){
        dbTable<-'tunaatlas.total_catches_ird_labels'
      } else {
        if(CatchTransformationLevel=="IRD" || is.null(CatchTransformationLevel)){
          dbTable<-'tunaatlas.catches_ird_rf1_labels'
        } else {
          dbTable<-'tunaatlas.catches_ird_raw_labels'
        }
      }
    }

    if (!is.null(TimeResolution)){
      if ( TimeResolution %in% c("month","quarter","semester") | ( TimeResolution %in% c("year","decade") & (AreaFilter_type!="rfmo" ) & !is.null(AreaFilter))){
        if(CatchTransformationLevel=="IRD" || is.null(CatchTransformationLevel)){
          dbTable<-'tunaatlas.catches_ird_rf1_labels'
        } else {
          dbTable<-'tunaatlas.catches_ird_raw_labels'
        }
      } else {
        dbTable<-'tunaatlas.total_catches_ird_labels'
      }
    }

  }


  #This last condition (schooltypefilter) should be removed if we use others tables than sardara as input
  if (!is.null(SchooltypeFilter) || !is.null(variable_to_group_by)){
    if (!is.null(SchooltypeFilter) || (length(grep("schooltype", variable_to_group_by))>0)){
      if(CatchTransformationLevel=="IRD" || is.null(CatchTransformationLevel)){
        dbTable<-'tunaatlas.catches_ird_rf1_labels'
      } else {
        dbTable<-'tunaatlas.catches_ird_raw_labels'
      }
    }
  }


  if (length(grep("area", variable_to_group_by))>0){
    if(CatchTransformationLevel=="IRD" || is.null(CatchTransformationLevel)){
      dbTable<-'tunaatlas.catches_ird_rf1_labels'
    } else {
      dbTable<-'tunaatlas.catches_ird_raw_labels'
    }
  }

  # SELECTION OF THE RIGHT COLUMNS

  #column value (fact)
  col_fact_value="v_catch"

  #column species

  # Get the right species column name in function of the species given in filter

  if (!is.null(SpeciesFilter)){
    sp_filter<-unlist(strsplit(SpeciesFilter, split=","))
    if (nchar(sp_filter[1])==3){
      SpeciesResolution='species'
    } else {
      SpeciesResolution='species group'
    }
  } else {
    if(!is.null(variable_to_group_by)){
      if (variable_to_group_by=="species" & !is.null(SpeciesResolution)){
        SpeciesResolution=SpeciesResolution
      } else {
        SpeciesResolution='species' }
    }
  }

  if(!is.null(SpeciesResolution)){

    if(SpeciesResolution=='species group'){
      col_species<-"id_speciesgroup_tunaatlas"
    }

    if(SpeciesResolution=='species'){
      col_species<-paste("id_",SpeciesResolution,"_",CodeListSource,sep="")
    }

  }


  #column gear

  if (!is.null(GearFilter)){
    ge_filter<-unlist(strsplit(GearFilter, split=","))
    if (nchar(ge_filter[1])==2){
      GearResolution='gear'
    } else {
      GearResolution='gear group'
    }
  } else {
    if(!is.null(variable_to_group_by)){
      if (variable_to_group_by=="gear" & !is.null(GearResolution)){
        GearResolution=GearResolution
      } else {
        GearResolution='gear' }
    }
  }

  if(!is.null(GearResolution)){
    if(GearResolution=='gear group'){
      # col_gear<-"id_geargroup_tunaatlas"
      # col_gear<-paste("id_",GearResolution,"_",CodeListSource,sep="")
      col_gear<-"id_geargroup_standard"

    }

    if(GearResolution=='gear'){
      col_gear<-paste("id_",GearResolution,"_",CodeListSource,sep="")
    }
  }

  ##############TO CHANGE IN THE FUTURE#######################it is not possible to distinguish with this code the "id_gear_rfmo" and "id_geargroup_tunaatlas" columns since they use the same codes. We have to find another way.for now we use only id_geargroup_tunaatlas
  col_gear<-"id_geargroup_standard"

  #column flag


  if (!is.null(FlagFilter)){
    fl_filter<-unlist(strsplit(FlagFilter, split=","))
    if (fl_filter[1] %in% c("Africa","Europe","Americas","Oceania","Other nei","Asia")){
      FlagResolution='continent'
    } else {
      FlagResolution='flag'
    }
  } else {
    if(!is.null(variable_to_group_by)){
      if (variable_to_group_by=="flag" & !is.null(FlagResolution)){
        FlagResolution=FlagResolution
      } else {
        FlagResolution='flag' }
    }
  }

  if(!is.null(FlagResolution)){
    if(FlagResolution=='continent'){
      col_flag<-"flag_continent"
    }

    if(FlagResolution=='flag'){
      col_flag<-paste("id_",FlagResolution,"_",CodeListSource,sep="")
    }
  }


  #column schooltype
  col_schooltype<-"id_schooltype"

  #column time start
  col_timestart<-"time_start"

  #column time end
  col_timeend<-"time_end"


  return(list(dbTable,col_fact_value,col_species,col_gear,col_flag,col_schooltype,col_timestart,col_timeend))

}



# Prepare a query to run on the database, depending on the parameters set by the user.

.PrepareQueryOnTable<-function(
  DataSource,  # table name in the database, e.g. tunaatlas.temp_table_catch
  col_fact_value,  # fact value column name (i.e. catch)
  col_species,  # species column name
  col_gear,  # gear column name
  col_flag,  # flag column name
  col_schooltype,  # schooltype column name
  col_timestart,  # time start column name
  col_timeend,  # time end column name
  TimeResolution,  # time resolution provided by the user
  TimeStart,  # Time start filter
  TimeEnd,  # Time end filter
  SpeciesFilter,  # species filter
  GearFilter,  # gear filter
  FlagFilter,  # flag filter
  AreaFilter,  # area filter
  SchooltypeFilter,  # schooltype filter
  variable_to_group_by,  # variables to group by. {species,area,flag,gear,schooltype,area,time}. combination of these variables is possible.
  SpatialIntersectionMethod  # Spatial method to use for the areas that intersect the area of interest
) {




  # SELECTION OF THE RIGHT TABLE

  dbTable<-DataSource


  #column time for aggregation

  if (is.null(TimeResolution)){
    if ((as.Date(TimeEnd)-as.Date(TimeStart))<733){  #733 days=2 years
      TimeResolution="month"
    } else {
      TimeResolution="year"
    }
  }

  if (TimeResolution %in% c("month","quarter","semester")){
    col_time<-paste(TimeResolution,",year",sep="")
  }

  if (TimeResolution %in% c("year","decade")){
    col_time<-TimeResolution
  }


  #### WHERE QUERY function

  # WHERE CLAUSE for time (filter time)
  time_query_where_clause<-paste(col_timestart,">='",TimeStart,"' AND ",col_timeend,"<='",TimeEnd,"'",sep="")

  # WHERE CLAUSE for species (filter species)
  species_query_where_clause<-.where_clause_function(col_species,SpeciesFilter)

  # WHERE CLAUSE for gears (filter gear)
  gear_query_where_clause<-.where_clause_function(col_gear,GearFilter)

  # WHERE CLAUSE for flags (filter flags)
  flag_query_where_clause<-.where_clause_function(col_flag,FlagFilter)

  # WHERE CLAUSE for schooltype (filter schooltype)
  schooltype_query_where_clause<-.where_clause_function(col_schooltype,SchooltypeFilter)

  # WHERE CLAUSE for areas (filter area)

  # If there is no area filter, then there is no WHERE area query
  if (is.null(AreaFilter)){
    area_query_where_clause<-NULL
    SpatialIntersectionMethod<-NULL
    AreaFilter_geometry_type="not_spatial_filter"
    col_area<-"geom"
  } else {

    #If there is an area filter, it can be of two types: WKT or a string.
    # It it is a WKT, then we use the wkt as input (AreaFilter_vquery)
    # If it is a string, it can be of two types:
    # - a code that is in the "id_area" column of the dataset: then we do a non geospatial area (i.e. we filter on the column id_area)
    # - other: then we look in the database (area schema) if there is a geom associated to the string. And we take the column geom as input (AreaFilter_vquery)
    if(substr(AreaFilter,(nchar(AreaFilter)+1)-1,nchar(AreaFilter))==")"){
      AreaFilter_geometry_type="wkt"
      AreaFilter_vquery<-paste("'",AreaFilter,"',4326",sep="")
      col_area<-"geom"
    } else {  #then it is a string
      # We check if the filter is a code in the "id_area" column of the dataset. If the first argument of the filter is available, then we assume they are all.
      query_get_area_codes<-paste("SELECT DISTINCT (id_area) FROM ",dbTable,sep="")
      areas_codes <- dbGetQuery(con, query_get_area_codes)$id_area
      filter=strsplit(AreaFilter,",")[[1]]
      if (filter[1] %in% areas_codes){
        col_area="id_area"
        AreaFilter_geometry_type="not_spatial_filter"
        area_query_where_clause<-.where_clause_function(col_area,AreaFilter)
      }
      else {
        # - or the name of an area in the DB
        col_area="geom"
        AreaFilter_geometry_type="geom"
        filter=gsub(",", "','", AreaFilter)
        filter=paste("('",filter,"')",sep="")
        AreaFilter_vquery <- paste("select ST_Union(geom) from area.areas_with_geom where codesource_area IN ",filter,sep="")
      }
    }


    ## at this point, we have the filter (AreaFilter_vquery) that is either the geom column or the wkt . Now we check which intersecting function to use
    if (AreaFilter_geometry_type %in% c("geom","wkt")) {
      ## at this point, we have the filter (AreaFilter_vquery) that is either the geom column or the wkt . Now we check which intersecting function to use

      if (is.null(SpatialIntersectionMethod)){ SpatialIntersectionMethod="proportion" }

      if (SpatialIntersectionMethod=="within"){
        postgis_intersect_function<-"ST_Within"
      }
      if (SpatialIntersectionMethod=="intersect" || SpatialIntersectionMethod=="proportion"){
        postgis_intersect_function<-"ST_Intersects"
      }

      # Finally we write the area query where clause.
      #if the filter type is a wkt, we use the postgis function ST_GEOMfromtext to convert it to geometry
      if (AreaFilter_geometry_type=="geom"){
        postgis_geomtype_function<-NULL
      } else if (AreaFilter_geometry_type=="wkt"){
        postgis_geomtype_function<-"ST_GeomFromText"
      }




      area_query_where_clause<-paste(" AND ",postgis_intersect_function,"(",dbTable,".",col_area,",",postgis_geomtype_function,"(",AreaFilter_vquery,"))",sep="")



    }
  }


  # WHERE CLAUSE global
  where_clause<-paste("WHERE ",time_query_where_clause,species_query_where_clause,flag_query_where_clause,gear_query_where_clause,schooltype_query_where_clause,area_query_where_clause)



  # QUERY: SELECT

  if (is.null(variable_to_group_by)){
    col_to_group_by<-"NULL::text"
  } else {
    col_to_group_by<-""
    if (length(grep("gear", variable_to_group_by))>0){
      col_to_group_by<-paste(col_to_group_by,col_gear,sep=",")
    }
    if (length(grep("flag", variable_to_group_by))>0){
      col_to_group_by<-paste(col_to_group_by,col_flag,sep=",")
    }
    if (length(grep("schooltype", variable_to_group_by))>0){
      col_to_group_by<-paste(col_to_group_by,col_schooltype,sep=",")
    }
    if (length(grep("species", variable_to_group_by))>0){
      col_to_group_by<-paste(col_to_group_by,col_species,sep=",")
    }
    if (length(grep("time", variable_to_group_by))>0){
      col_to_group_by<-paste(col_to_group_by,col_time,sep=",")
    }
    if (length(grep("area", variable_to_group_by))>0){
      col_to_group_by<-paste(col_to_group_by,"geom",sep=",")
    }
    col_to_group_by<-substring(col_to_group_by, 2)
  }


  select_clause<-col_to_group_by



  ### QUERY: GROUP BY

  groupby_clause<-paste("GROUP BY ",select_clause,sep="")



  ## QUERY: JOIN CLAUSE
  #No join clause
  join_clause<-NULL

  # QUERY:

  if (is.null(area_query_where_clause) || SpatialIntersectionMethod!="proportion" || AreaFilter_geometry_type=="not_spatial_filter"){
    query<-paste("SELECT trunc(sum(",col_fact_value,")::numeric,2) as value,",select_clause," FROM ",dbTable," ",join_clause," ",where_clause," ",groupby_clause,sep="")
  } else {
    query<-paste("SELECT sum(trunc(",col_fact_value,"::numeric,2) * ST_Area(ST_Intersection(",postgis_geomtype_function,"(",AreaFilter_vquery,"),",dbTable,".",col_area,"))/ST_Area(",dbTable,".",col_area,")) as value,",select_clause," FROM ",dbTable," ",join_clause," ",where_clause," ",groupby_clause,sep="")
  }


  return(list(query,select_clause,AreaFilter_geometry_type,DataSource,where_clause,col_area,SpatialIntersectionMethod))

}





#get the results of a query with the selected geometry type (coord_centroid,wkt,postgis_geometry,shapefile)

.GetGeomOutputFromQuery<-function(query,  # output[[1]] of the function Sardara_GetColumnsAndTableNamesForQuery
                                 select_clause,  # output[[2]] of the function Sardara_GetColumnsAndTableNamesForQuery
                                 SpaceOutputFormat  #NULL,coord_centroid,wkt,postgis_geometry,shapefile   or combination

) {

  geom_ouput<-""

  if ("NULL" %in% SpaceOutputFormat){
    geom_ouput<-""
  }
  if ("coord_centroid" %in% SpaceOutputFormat){
    geom_ouput<-paste(geom_ouput,",ST_X(ST_Centroid(geom)) as long_centroid,ST_Y(ST_Centroid(geom)) as lat_centroid",sep="")
  }
  if ("wkt" %in% SpaceOutputFormat){
    geom_ouput<-paste(geom_ouput,",ST_AsText(geom) as wkt_area",sep="")
  }
  if ("postgis_geometry" %in% SpaceOutputFormat){
    geom_ouput<-paste(geom_ouput,",geom",sep="")
  }
  if ("shapefile" %in% SpaceOutputFormat){

  }

  select_clause<-gsub(",geom","",select_clause)
  query<-paste("WITH query as (",query,") SELECT value,",select_clause,geom_ouput," FROM query ",sep="")

  return(query)

}


#get the distinct geometries resulting of a query with the selected geometry type (coord_centroid,wkt,postgis_geometry,shapefile)

.GetDistinctGeomOutputFromQuery<-function(DataSource,  # output[[4]] of the function Sardara_GetColumnsAndTableNamesForQuery
                                         col_area,  # output[[6]] of the function Sardara_GetColumnsAndTableNamesForQuery
                                         where_clause,  # output[[5]] of the function Sardara_GetColumnsAndTableNamesForQuery
                                         SpaceOutputFormat  #NULL,coord_centroid,wkt,postgis_geometry,shapefile   or combination

) {

  geom_ouput<-""

  if ("NULL" %in% SpaceOutputFormat){
    geom_ouput<-""
  }
  if ("coord_centroid" %in% SpaceOutputFormat){
    geom_ouput<-paste(geom_ouput,",ST_X(ST_Centroid(geom)),ST_Y(ST_Centroid(geom))",sep="")
  }
  if ("wkt" %in% SpaceOutputFormat){
    geom_ouput<-paste(geom_ouput,",ST_AsText(geom)",sep="")
  }
  if ("postgis_geometry" %in% SpaceOutputFormat){
    geom_ouput<-paste(geom_ouput,",geom",sep="")
  }
  if ("shapefile" %in% SpaceOutputFormat){

  }

  geom_ouput<-substring(geom_ouput, 2)

  query<-paste("SELECT DISTINCT(",geom_ouput,") as geometry FROM ",DataSource," ",where_clause,sep="")

  return(query)

}



#project a df with a geometry on another table for which the mapping has already been done in the table area.area_mapping of the database
.ProjectResultsOnAreaTableAlreadyMapped<-function(
  SourceQuery, # query on the source table . output of the function GetGeomOutputFromQuery
  select_clause, # select clause. output[[2]] of PrepareQueryOnTable
  tablesource_AreaColumnName, #name of area column on the source table
  area_mapping_relation_type #name of the mapping to use on the area_mapping table

){

  SourceQuery<-gsub("\\b,geom\\b", ",id_area", SourceQuery) #\\b is to indicate word boundaries
  select_clause<-gsub(",geom", "", select_clause)

  query<-paste("WITH table_source as (",SourceQuery,"), table_target as (select sum(percentage_intersect*value) as value,",select_clause,",area_mapping_id_to FROM table_source JOIN area.area ON (table_source.id_area=area.codesource_area) JOIN area.area_mapping ON (area_mapping.area_mapping_id_from=area.id_area) WHERE area_mapping.area_mapping_relation_type='",area_mapping_relation_type,"' group by ",select_clause,",area_mapping_id_to) SELECT value,",select_clause,",geom FROM table_target JOIN area.areas_with_geom ON table_target.area_mapping_id_to=areas_with_geom.id_area",sep="")

  return(query)

}

getSpeciesSimple <- function() {
  library(jsonlite)
  library(plyr)
  library(RCurl)
  library (DBI)
  library ("RPostgreSQL")
  query <- "select
    x3a_code as id,
    english_name as name,
    scientific_name as scientific_name,
    from species.species_asfis
      where x3a_code in (select distinct(id_species_standard) from tunaatlas.catches_ird_rf1_labels)
    order by english_name"
  drv <- dbDriver("PostgreSQL")
  con_sardara <- dbConnect(drv, user = "invsardara",password="fle087",dbname="sardara_world",host ="db-tuna.d4science.org",port=5432)
  species <- dbGetQuery(con_sardara, query)
  return (toJSON(species))
}


getDataByFilters <- function(
  SourceData = "Sardara",  # Sardara, Generic
  SpeciesFilter = "YFT",
  GearFilter = "NULL",
  FlagFilter = "NULL",
  SchooltypeFilter = "NULL",  # {NULL , Free school, Log school, Dolphin, Undefined school }
  TimeStart = "1950-01-01",
  TimeEnd = "2015-01-01",
  TimeResolution = "year",    # {Automatic choice, month , year, quarter, semester, decade}
  AreaFilter = "NULL",
  SpatialIntersectionMethod = "Automatic choice",  # {Automatic choice, intersect , within , proportion}
  SpeciesResolution = "NULL",   # {species , species group}
  GearResolution = "gear group",  # {gear , gear group}
  FlagResolution = "flag",  # {flag , continent }
  CatchTransformationLevel = "Automatic choice",   # {Automatic choice, raw,IRD}
  CodeListSource = "standard",  # NOT WORKING YET {standard,rfmo}
  GridResolution = "5 x 5"
) {
    library("RPostgreSQL")
    library(jsonlite)
  TimeStart = "2010-01-01"
    variable_to_group_by<-"area,time"  # {gear,flag,schooltype,species,NULL,area,time  or combination of all}
    DataSource<-"NULL"

    #### Is the input a generic dataset or a Tuna Atlas dataset?
    #SourceData="Sardara"  # Sardara, Generic

    #### Variables declaration
    #SpeciesFilter="NULL"
    #GearFilter="NULL"
    #FlagFilter="NULL"
    #SchooltypeFilter="NULL"  # {NULL , Free school, Log school, Dolphin, Undefined school }
    #TimeStart="1950-01-01"
    #TimeEnd="2015-01-01"
    #TimeResolution="year"    # {Automatic choice, month , year, quarter, semester, decade}
    #AreaFilter = "NULL"
    #SpatialIntersectionMethod="Automatic choice"  # {Automatic choice, intersect , within , proportion}


    #### variables specific for Sardara##
    #SpeciesResolution="NULL"   # {species , species group}
    #GearResolution="gear group"  # {gear , gear group}
    #FlagResolution="flag"  # {flag , continent }
    #CatchTransformationLevel="Automatic choice"   # {Automatic choice, raw,IRD}
    #CodeListSource="standard"  # NOT WORKING YET {standard,rfmo}
    #GridResolution="5 x 5"
    ####

    ## BEGIN
    ## set "NULL" variable to NULL
    variable_to_group_by<-.CheckNullVariable(variable_to_group_by)
    SpeciesFilter<-.CheckNullVariable(SpeciesFilter)
    GearFilter<-.CheckNullVariable(GearFilter)
    FlagFilter<-.CheckNullVariable(FlagFilter)
    SchooltypeFilter<-.CheckNullVariable(SchooltypeFilter)
    TimeStart<-.CheckNullVariable(TimeStart)
    TimeEnd<-.CheckNullVariable(TimeEnd)
    TimeResolution<-.CheckNullVariable(TimeResolution)
    AreaFilter<-.CheckNullVariable(AreaFilter)
    SpatialIntersectionMethod<-.CheckNullVariable(SpatialIntersectionMethod)

    ##variables specific for Sardara##
    SpeciesResolution<-.CheckNullVariable(SpeciesResolution)
    FlagResolution<-.CheckNullVariable(FlagResolution)
    DataSource<-.CheckNullVariable(DataSource)
    CatchTransformationLevel<-.CheckNullVariable(CatchTransformationLevel)
    CodeListSource<-.CheckNullVariable(CodeListSource)
    ####


    drv <- dbDriver("PostgreSQL")
    con <- dbConnect(drv, dbname="sardara_world", user="invsardara", password="fle087", host="db-tuna.d4science.org")



    if (SourceData=="Sardara"){

      SardaraColumnsToUse<-.Sardara_GetColumnsAndTableNamesForQuery(DataSource,
                                                                    CatchTransformationLevel,
                                                                    CodeListSource,
                                                                    AreaFilter,
                                                                    variable_to_group_by,
                                                                    TimeStart,
                                                                    TimeEnd,
                                                                    TimeResolution,
                                                                    SchooltypeFilter,
                                                                    SpeciesFilter,
                                                                    SpeciesResolution,
                                                                    GearFilter,
                                                                    GearResolution,
                                                                    FlagFilter,
                                                                    FlagResolution)


      DataSource=SardaraColumnsToUse[[1]]
      col_fact_value=SardaraColumnsToUse[[2]]
      col_species=SardaraColumnsToUse[[3]]
      col_gear=SardaraColumnsToUse[[4]]
      col_flag=SardaraColumnsToUse[[5]]
      col_schooltype=SardaraColumnsToUse[[6]]
      col_timestart=SardaraColumnsToUse[[7]]
      col_timeend=SardaraColumnsToUse[[8]]

    } else if (SourceData=="Generic"){
      DataSource=DataSource
      col_fact_value="v_catch"
      col_species="id_species"
      col_gear="id_gear"
      col_flag="id_flag"
      col_schooltype="id_schooltype"
      col_timestart="time_start"
      col_timeend="time_end"

    }



    # Prepare the query to execute
    Query<-.PrepareQueryOnTable(DataSource=DataSource,
                                col_fact_value=col_fact_value,
                                col_species=col_species,
                                col_gear=col_gear,
                                col_flag=col_flag,
                                col_schooltype=col_schooltype,
                                col_timestart=col_timestart,
                                col_timeend=col_timeend,
                                TimeResolution=TimeResolution,
                                TimeStart=TimeStart,
                                TimeEnd=TimeEnd,
                                SpeciesFilter=SpeciesFilter,
                                GearFilter=GearFilter,
                                FlagFilter=FlagFilter,
                                AreaFilter=AreaFilter,
                                SchooltypeFilter=SchooltypeFilter,
                                variable_to_group_by=paste(variable_to_group_by,"area"),
                                SpatialIntersectionMethod=SpatialIntersectionMethod )

    # If we use Sardara as input and a 5° x 5° grid resolution to project the data, we do here-under process
    if (SourceData=="Sardara"){
      if (GridResolution=="5 x 5"){
        Query[[1]]<-.ProjectResultsOnAreaTableAlreadyMapped(
          SourceQuery=Query[[1]], # query on the source table . output[[1]] of the function GetGeomOutputFromQuery
          select_clause=Query[[2]], # select clause. output[[2]] of PrepareQueryOnTable
          tablesource_AreaColumnName="id_area", #name of area column on the source table
          area_mapping_relation_type="cwpgrid_1deg_2_5deg" #name of the mapping to use on the area_mapping table
        )
      }
      if (GridResolution=="1 x 1"){
        Query[[1]]<-gsub("WHERE",paste("WHERE left(",Query[[4]],".id_area,1)='5' AND ",sep=""),Query[[1]])
      }

    }

    # FinalQuery is to get the geom in the right format associated to the query
    FinalQuery<-.GetGeomOutputFromQuery(query=Query[[1]],select_clause=Query[[2]],SpaceOutputFormat=c("wkt"))
    #Execute the query and get the output dataframe
    print(FinalQuery)
    DfForMap <- dbGetQuery(con, FinalQuery)

    colnames(DfForMap)<-c("CatchWeightT","SeasonYear","Polygon")
    drops <-        c()

    yearsT <- sort(unique(DfForMap[['SeasonYear']], incomparables = FALSE))

    res <- list()

    for (yearT in yearsT) {
      t <- DfForMap[DfForMap$SeasonYear == yearT, ]
      t <- t[ , !(names(t) %in% drops)]
      t <- aggregate(t[,c("CatchWeightT")], by=list(t$Polygon), "sum")
      t <- t[with(t, order(-x, Group.1)), ]
      colnames(t)<-c("Polygon","CatchWeightT")

      max_catch <- max(t$CatchWeightT, na.rm = TRUE)
      min_catch <- min(t$CatchWeightT, na.rm = TRUE)

      n_col = 30

      a=(1-n_col)/(sqrt(min_catch)-sqrt(max_catch))
      b=n_col-a*sqrt(max_catch)

      t$color <- round(a*sqrt(t$CatchWeightT)+b)

      res[[toString(yearT)]] <- t
    }
    return (toJSON(res, pretty = FALSE))
}
