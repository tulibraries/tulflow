<?xml version="1.0" encoding="UTF-8"?>
<!-- Funcake name: 'Villanova' -->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xmlns:dc="http://purl.org/dc/elements/1.1/"
    xmlns:dcterms="http://purl.org/dc/terms/"
    xmlns:dpla="http://dp.la/about/map/"
    xmlns:padig="http://padigital.org/ns"
    xmlns:edm="http://www.europeana.eu/schemas/edm/"
    xmlns:oclcdc="http://worldcat.org/xmlschemas/oclcdc-1.0/"
    xmlns:oclcterms="http://purl.org/oclc/terms/"
    xmlns:oai="http://www.openarchives.org/OAI/2.0/"
    xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
    xmlns:oclc="http://purl.org/oclc/terms/"
    xmlns:oai_qdc="http://worldcat.org/xmlschemas/qdc-1.0/"
    xmlns:schema="http://schema.org"
    xmlns:svcs="http://rdfs.org/sioc/services"
    version="2.0">
    <xsl:output omit-xml-declaration="no" method="xml" encoding="UTF-8" indent="yes"/>

    <!-- drop nodes we don't care about, namely, header values -->
    <xsl:template match="text() | @*"/>

    <!-- drop records where the OAI header is marked as 'deleted' -->
    <xsl:template match="//oai:record[oai:header[@status='deleted']]/*"/>

    <!-- base record. Matches each OAI feed record that is mapped. Filters out records with dc:identifier values contained in remediation_filter.xsl -->
    <xsl:template match="//oai_dc:dc[not(dc:relation[string()= 'pdcp_noharvest'])]">
        <oai_dc:dc
            xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xmlns:dc="http://purl.org/dc/elements/1.1/"
            xmlns:dcterms="http://purl.org/dc/terms/"
            xmlns:dpla="http://dp.la/about/map/"
            xmlns:edm="http://www.europeana.eu/schemas/edm/"
            xmlns:oai="http://www.openarchives.org/OAI/2.0/"
            xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
            xmlns:oai_qdc="http://worldcat.org/xmlschemas/qdc-1.0/"
            xmlns:oclc="http://purl.org/oclc/terms/"
            xmlns:oclcdc="http://worldcat.org/xmlschemas/oclcdc-1.0/"
            xmlns:oclcterms="http://purl.org/oclc/terms/"
            xmlns:schema="http://schema.org" >

            <!-- will match specific templates that relevant for dplah. -->
            <xsl:apply-templates />
        </oai_dc:dc>
    </xsl:template>

     <!-- Title -->
    <xsl:template match="dc:title">
        <xsl:if test="normalize-space(.)!=''">
            <xsl:element name="dcterms:title">
                <xsl:value-of select="normalize-space(.)"/>
            </xsl:element>
        </xsl:if>
    </xsl:template>

    <!-- Type -->
    <xsl:template match="dc:type">
        <xsl:if test="normalize-space(.)!=''">
            <xsl:choose>
                <xsl:when test="matches(., '(^text.*$)', 'i')">
                    <dcterms:type>Text</dcterms:type>
                </xsl:when>
                <xsl:when test="matches(., '(^image.*$)', 'i')">
                    <dcterms:type>Image</dcterms:type>
                </xsl:when>
                <xsl:when test="matches(., '^(movingimage.*$|moving\simage.*$)', 'i')">
                    <dcterms:type>Moving Image</dcterms:type>
                </xsl:when>
                <xsl:when test="matches(., '^(sound.*$)', 'i')">
                    <dcterms:type>Sound</dcterms:type>
                </xsl:when>
                <xsl:when test="matches(., '^(physicalobject.*$|physical\sobject.*$)', 'i')">
                    <dcterms:type>Physical Object</dcterms:type>
                </xsl:when>
                <xsl:when
                    test="matches(., '^(interactiveresource.*$|interactive\sresource.*$)', 'i')">
                    <dcterms:type>Interactive Resource</dcterms:type>
                </xsl:when>
                <xsl:when
                    test="matches(., '^(stillimage.*$|still\simage.*$)', 'i')">
                    <dcterms:type>Still Image</dcterms:type>
                </xsl:when>
                <!-- Format -->
                <xsl:otherwise>
                    <dcterms:format>
                        <xsl:value-of select="."/>
                    </dcterms:format>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:if>
    </xsl:template>
  </xsl:stylesheet>
