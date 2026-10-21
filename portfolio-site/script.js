const menuToggle = document.getElementById("menuToggle");
const mainNav = document.getElementById("mainNav");

menuToggle.addEventListener("click", () => {
  const isOpen = mainNav.classList.toggle("open");
  menuToggle.setAttribute("aria-expanded", String(isOpen));
  menuToggle.textContent = isOpen ? "×" : "☰";
});

mainNav.querySelectorAll("a").forEach((link) => {
  link.addEventListener("click", () => {
    mainNav.classList.remove("open");
    menuToggle.setAttribute("aria-expanded", "false");
    menuToggle.textContent = "☰";
  });
});


/* Interactive pipeline walkthrough: explanatory content only, no fake live metrics. */
const stageContent = {
  ingest: {kicker:"STAGE 01 / INGESTION",title:"Bring source data in safely.",description:"Load synthetic customer and transaction files, including an incremental batch, as the inputs to the pipeline.",focus:"Repeatable input handling",output:"Raw source files ready for processing",tech:"Python · CSV · AWS S3"},
  transform: {kicker:"STAGE 02 / PROCESSING",title:"Transform data with Spark.",description:"Apply batch and incremental processing to the synthetic banking data using the project's PySpark pipeline.",focus:"Consistent, repeatable transformations",output:"Processed customer and transaction datasets",tech:"Python · PySpark · Spark SQL"},
  quality: {kicker:"STAGE 03 / DATA QUALITY",title:"Catch issues before downstream use.",description:"Run data-quality checks and anomaly screening, then retain a machine-readable report for the run.",focus:"Validation and anomaly screening",output:"Quality results and diagnostic evidence",tech:"Python · Data quality checks"},
  reconcile: {kicker:"STAGE 04 / RECONCILIATION",title:"Compare results across processing paths.",description:"Use reconciliation checks to help identify mismatches between pipeline outputs and expected results.",focus:"Cross-engine and output consistency",output:"Reconciliation outcome",tech:"SQL · Spark · Reconciliation checks"},
  operate: {kicker:"STAGE 05 / ORCHESTRATION",title:"Make the run observable and repeatable.",description:"Run the pipeline through Docker-based Airflow orchestration, validate the generated report, and upload raw inputs to S3.",focus:"Orchestration, status validation and recovery",output:"Pipeline run report and S3 ingestion result",tech:"Docker · Apache Airflow · AWS S3"}
};
const stageButtons = Array.from(document.querySelectorAll(".stage-button"));
const stagePanel = document.getElementById("stage-detail");
stageButtons.forEach((button) => {
  button.addEventListener("click", () => {
    const data = stageContent[button.dataset.stage];
    if (!data) return;
    stageButtons.forEach((item) => {
      const active = item === button;
      item.classList.toggle("active", active);
      item.setAttribute("aria-selected", String(active));
    });
    stagePanel.setAttribute("aria-labelledby", button.id);
    ["kicker","title","description","focus","output","tech"].forEach((key) => {
      const node = document.getElementById("stage-" + key);
      if (node) node.textContent = data[key];
    });
  });
});

/* Project filters keep the public work scannable for technical recruiters. */
const filterButtons = Array.from(document.querySelectorAll(".filter-button"));
const projectCards = Array.from(document.querySelectorAll(".project-grid .project-card"));
filterButtons.forEach((button) => {
  button.addEventListener("click", () => {
    const filter = button.dataset.filter;
    filterButtons.forEach((item) => {
      const active = item === button;
      item.classList.toggle("active", active);
      item.setAttribute("aria-pressed", String(active));
    });
    projectCards.forEach((card) => {
      const categories = (card.dataset.categories || "").split(/\s+/);
      card.hidden = filter !== "all" && !categories.includes(filter);
    });
  });
});


/* Build-refreshed dataset metrics and interactive illustrative scale chart. */
(async function initializeScaleDashboard() {
  const countNode = document.getElementById("metric-customers");
  const txNode = document.getElementById("metric-transactions");
  const footprintNode = document.getElementById("metric-footprint");
  const updatedNode = document.getElementById("metric-updated");
  const chart = document.getElementById("dataset-chart");
  const formatCount = (n) => new Intl.NumberFormat("en-US").format(n);
  const formatBytes = (n) => n >= 1e9 ? (n / 1e9).toFixed(2) + " GB" : n >= 1e6 ? (n / 1e6).toFixed(2) + " MB" : n >= 1e3 ? (n / 1e3).toFixed(1) + " KB" : n + " B";
  const svgEl = (tag, attrs, text) => {
    const el = document.createElementNS("http://www.w3.org/2000/svg", tag);
    Object.entries(attrs || {}).forEach(([k,v]) => el.setAttribute(k, String(v)));
    if (text !== undefined) el.textContent = text;
    return el;
  };
  function drawChart(customers, transactions, customerBytes = null, transactionBytes = null) {
    if (!chart) return;
    chart.replaceChildren();
    chart.setAttribute("viewBox","0 0 600 292");
    chart.append(svgEl("text",{x:18,y:20,fill:"#8e9fbd","font-size":10,"letter-spacing":1.2},"RECORD COUNTS"));
    const rows=[{label:"Customers",value:customers,color:"#a99bff",y:35},{label:"Transactions",value:transactions,color:"#6bdcf2",y:78}];
    const rowMax=Math.max(1,...rows.map(v=>v.value));
    rows.forEach(v=>{
      const width=Math.max(3,Math.round(430*v.value/rowMax));
      chart.append(svgEl("text",{x:18,y:v.y+2,fill:"#b7c5dc","font-size":11},v.label));
      chart.append(svgEl("rect",{x:18,y:v.y+9,width:430,height:15,rx:5,fill:"#1b263b"}));
      chart.append(svgEl("rect",{x:18,y:v.y+9,width,height:15,rx:5,fill:v.color,opacity:.95}));
      chart.append(svgEl("text",{x:578,y:v.y+21,fill:"#eef3ff","font-size":11,"text-anchor":"end"},formatCount(v.value)));
    });
    chart.append(svgEl("line",{x1:18,y1:119,x2:580,y2:119,stroke:"#29364d","stroke-width":1}));
    chart.append(svgEl("text",{x:18,y:142,fill:"#8e9fbd","font-size":10,"letter-spacing":1.2},"RAW FILE SIZE"));
    if (Number.isFinite(customerBytes) && Number.isFinite(transactionBytes)) {
      const sizes=[
        {label:"customers.csv",value:customerBytes,color:"#a99bff",y:157},
        {label:"transactions.csv",value:transactionBytes,color:"#6bdcf2",y:201}
      ];
      const sizeMax=Math.max(1,...sizes.map(v=>v.value));
      sizes.forEach(v=>{
        const width=Math.max(3,Math.round(430*v.value/sizeMax));
        chart.append(svgEl("text",{x:18,y:v.y+2,fill:"#b7c5dc","font-size":11},v.label));
        chart.append(svgEl("rect",{x:18,y:v.y+9,width:430,height:15,rx:5,fill:"#1b263b"}));
        chart.append(svgEl("rect",{x:18,y:v.y+9,width,height:15,rx:5,fill:v.color,opacity:.85}));
        chart.append(svgEl("text",{x:578,y:v.y+21,fill:"#eef3ff","font-size":11,"text-anchor":"end"},formatBytes(v.value)));
      });
      chart.append(svgEl("text",{x:18,y:258,fill:"#7184a4","font-size":9},"Measured from the latest generated CSV files"));
    } else {
      chart.append(svgEl("text",{x:18,y:177,fill:"#7184a4","font-size":11},"File-size bars appear after the deployment metrics load."));
      chart.append(svgEl("text",{x:18,y:258,fill:"#7184a4","font-size":9},"Counts shown from the synthetic dataset configuration"));
    }
    chart.append(svgEl("text",{x:18,y:281,fill:"#7184a4","font-size":9},"Synthetic portfolio data · refreshed at deploy time"));
  }
  // Render a known synthetic baseline immediately; replace it with exact build metrics when fetched.
  if (countNode) countNode.textContent = formatCount(500);
  if (txNode) txNode.textContent = formatCount(10004);
  if (footprintNode) footprintNode.textContent = "Refreshing…";
  if (updatedNode) updatedNode.textContent = "Showing configured synthetic counts while exact file sizes refresh.";
  drawChart(500, 10004);
  try {
    const response = await fetch(new URL("data/pipeline-metrics.json?build=" + Date.now(), document.baseURI), {cache:"no-store"});
    if (!response.ok) throw new Error("metrics unavailable");
    const metrics = await response.json();
    if (countNode) countNode.textContent = formatCount(metrics.customers.rows);
    if (txNode) txNode.textContent = formatCount(metrics.transactions.rows);
    if (footprintNode) footprintNode.textContent = formatBytes(metrics.total_bytes);
    if (updatedNode) updatedNode.textContent = "Generated from CSV files during deployment · " + new Date(metrics.generated_at).toLocaleString();
    drawChart(metrics.customers.rows, metrics.transactions.rows, metrics.customers.bytes, metrics.transactions.bytes);
  } catch (_) {
    if (countNode) countNode.textContent = formatCount(500);
    if (txNode) txNode.textContent = formatCount(10004);
    if (footprintNode) footprintNode.textContent = "Build pending";
    if (updatedNode) updatedNode.textContent = "Synthetic dataset counts · exact file sizes refresh after deployment.";
    drawChart(500, 10004);
  }


})();


/* Technology carousel: selected stack item rotates into the centre; graph root stays centred. */
(() => {
  const wheel = document.getElementById("techWheel");
  const explorer = document.getElementById("technology-explorer");
  const graph = document.getElementById("technology-graph");
  const overviewTrigger = document.getElementById("coreTechTrigger");
  const closeButton = document.getElementById("techMapClose");
  if (!wheel || !explorer || !graph || !overviewTrigger) return;

  const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  const techButtons = Array.from(wheel.querySelectorAll(".technology-chip"));
  const getButtonsInVisualOrder = () => Array.from(wheel.querySelectorAll(".technology-chip"));
  const views = {
    overview: {
      type:"STACK OVERVIEW",root:"Data Engineering Stack",title:"Connected delivery stack",
      description:"The technologies work together across ingestion, distributed processing, lakehouse storage, orchestration, cloud services, governance and quality controls.",
      nodes:[["Spark & PySpark","Batch + streaming"],["Databricks & Delta","Lakehouse tables"],["AWS data services","Storage + compute"],["SQL + ETL / ELT","Transform + model"],["Orchestration","Scheduling + retries"],["Quality & lineage","Validation + traceability"]],
      tags:["Spark","Databricks","AWS","SQL","ETL / ELT","Delta Lake","Data Quality"]
    },
    spark: {
      type:"DISTRIBUTED PROCESSING",root:"Apache Spark",title:"Spark processing ecosystem",
      description:"Distributed batch transformations, SQL processing, streaming ingestion and performance tuning across banking-data workloads.",
      nodes:[["PySpark / DataFrames","Python processing"],["Scala","JVM workloads"],["Spark SQL","Query transformations"],["Structured Streaming","Event processing"],["Kafka","Streaming integration"],["Hive / HDFS","Data lake ecosystem"]],
      tags:["Partitioning","Caching","Broadcast joins","AQE","Predicate pushdown","Batch + streaming"]
    },
    databricks: {
      type:"LAKEHOUSE PLATFORM",root:"Databricks",title:"Lakehouse and governance",
      description:"Databricks and Delta technologies support reliable table processing, incremental patterns, access controls and governed analytics.",
      nodes:[["Delta Lake","ACID table storage"],["Unity Catalog","Access + lineage"],["Workflows","Job orchestration"],["Medallion design","Bronze / Silver / Gold"],["AWS S3","Object storage"],["Delta pipelines","Managed transformation"]],
      tags:["Incremental processing","Table quality","RBAC","Lineage","Lakehouse"]
    },
    languages: {
      type:"LANGUAGES & INTERFACES",root:"PySpark / Scala",title:"Languages and interfaces",
      description:"Language and query layers connect reusable pipeline logic, SQL transformations, automation scripts and version-controlled delivery.",
      nodes:[["Python","Pipeline utilities"],["SQL / Spark SQL","Data transformation"],["Shell scripting","Automation"],["DataFrames","Distributed API"],["Git","Version control"],["Parquet / JSON","Data formats"]],
      tags:["Reusable jobs","Testing","Debugging","Automation"]
    },
    aws: {
      type:"CLOUD DATA SERVICES",root:"AWS",title:"Cloud storage and compute",
      description:"Cloud services cover object storage, distributed compute, event streaming, cataloguing, querying and permission boundaries.",
      nodes:[["Amazon S3","Object storage"],["Amazon EMR","Managed Spark"],["Amazon EC2","Compute"],["IAM","Identity + access"],["Glue / Lambda","Catalog + functions"],["Athena / Redshift","Query + warehouse"]],
      tags:["Kinesis","Cloud migration","S3 ingestion","Access control"]
    },
    sql: {
      type:"DATA QUERY & MODELING",root:"SQL",title:"Query, validate, model",
      description:"SQL supports transformation logic, reconciliation, aggregation and analytical outputs alongside Spark SQL and data-quality checks.",
      nodes:[["Spark SQL","Distributed query"],["Hive SQL","Warehouse queries"],["DuckDB","Local analytics engine"],["Reconciliation","Cross-path checks"],["Dimensional models","Facts + dimensions"],["Quality assertions","Rules + validations"]],
      tags:["Aggregations","Window functions","CTEs","Reconciliation","Data modeling"]
    },
    etl: {
      type:"PIPELINE DESIGN",root:"ETL / ELT",title:"Reliable data movement",
      description:"The ingestion and transformation pattern determines where data is validated, how changes are processed, and how failures are recovered.",
      nodes:[["Extract / ingest","Source acquisition"],["Transform","Business rules"],["Load / publish","Destination tables"],["Incremental loads","Process new changes"],["Idempotency","Safe re-runs"],["Data quality","Reject + quarantine"]],
      tags:["Batch processing","Incremental ETL","Retries","Reconciliation","Audit trail"]
    },
    delta: {
      type:"LAKEHOUSE STORAGE",root:"Delta Lake",title:"Reliable lakehouse tables",
      description:"Delta Lake provides table-oriented storage patterns for incremental processing, consistent writes and quality-controlled analytical datasets.",
      nodes:[["ACID transactions","Consistent writes"],["MERGE","Upserts + CDC"],["Time travel","Versioned data"],["Schema enforcement","Safer writes"],["OPTIMIZE","File layout tuning"],["Unity Catalog","Governed access"]],
      tags:["Bronze / Silver / Gold","Incremental pipelines","Schema controls","Performance tuning"]
    }
  };

  const svgNS = "http://www.w3.org/2000/svg";
  const makeSvg = (tag, attrs, text) => {
    const element = document.createElementNS(svgNS, tag);
    Object.entries(attrs).forEach(([key,value]) => element.setAttribute(key,String(value)));
    if (text !== undefined) element.textContent = text;
    return element;
  };
  const addText = (parent,x,y,text,className) => parent.append(makeSvg("text",{x,y,class:className || ""},text));

  function drawView(data) {
    graph.replaceChildren();
    graph.setAttribute("viewBox","0 0 940 330");
    const root={x:350,y:128,w:240,h:72,cx:470,cy:164};
    const positions=[
      {x:20,y:20,w:200,h:60},{x:370,y:9,w:200,h:60},{x:720,y:20,w:200,h:60},
      {x:20,y:250,w:200,h:60},{x:370,y:261,w:200,h:60},{x:720,y:250,w:200,h:60}
    ];
    positions.forEach(pos=>{
      const nodeCx=pos.x+pos.w/2, nodeCy=pos.y+pos.h/2;
      const sourceX=nodeCx<root.cx?root.x:nodeCx>root.cx?root.x+root.w:root.cx;
      const sourceY=nodeCy<root.cy?root.y:root.y+root.h;
      graph.append(makeSvg("line",{x1:sourceX,y1:sourceY,x2:nodeCx,y2:nodeCy,class:"tech-graph-edge"}));
    });
    positions.forEach((pos,index)=>{
      const [label,detail]=data.nodes[index];
      const group=makeSvg("g",{class:"tech-graph-node","data-node":label});
      group.append(makeSvg("rect",{x:pos.x,y:pos.y,width:pos.w,height:pos.h,rx:9}));
      addText(group,pos.x+pos.w/2,pos.y+25,label,"");
      addText(group,pos.x+pos.w/2,pos.y+43,detail,"node-index");
      graph.append(group);
    });
    const rootGroup=makeSvg("g",{class:"tech-graph-node tech-graph-root"});
    rootGroup.append(makeSvg("rect",{x:root.x,y:root.y,width:root.w,height:root.h,rx:12}));
    addText(rootGroup,root.cx,root.cy-5,data.root,"");
    addText(rootGroup,root.cx,root.cy+17,"SELECTED TECHNOLOGY","node-index");
    graph.append(rootGroup);
  }

  function centerTechnology(key) {
    const orderedBefore = getButtonsInVisualOrder();
    const selected = orderedBefore.find(button => button.dataset.techSelect === key);
    if (!selected) return;
    const before = new Map(orderedBefore.map(button => [button,button.getBoundingClientRect().left]));
    const mid = Math.floor(orderedBefore.length/2);
    const selectedIndex = orderedBefore.indexOf(selected);
    const start = (selectedIndex - mid + orderedBefore.length) % orderedBefore.length;
    const order = orderedBefore.slice(start).concat(orderedBefore.slice(0,start));
    order.forEach(button => wheel.append(button));
    if (!reducedMotion) {
      order.forEach(button => {
        const delta = before.get(button) - button.getBoundingClientRect().left;
        if (Math.abs(delta) > 1 && typeof button.animate === "function") {
          button.animate([{transform:"translateX("+delta+"px)"},{transform:"translateX(0)"}],{duration:360,easing:"cubic-bezier(.2,.8,.2,1)"});
        }
      });
    }
    if (window.matchMedia("(max-width: 1000px)").matches || wheel.scrollWidth > wheel.clientWidth) {
      const wheelRect = wheel.getBoundingClientRect();
      const buttonRect = selected.getBoundingClientRect();
      const delta = buttonRect.left + buttonRect.width/2 - (wheelRect.left + wheelRect.width/2);
      wheel.scrollTo({left:wheel.scrollLeft + delta,behavior:reducedMotion?"auto":"smooth"});
    }
  }

  function selectTechnology(key,shouldScroll=true) {
    const data=views[key] || views.overview;
    explorer.hidden=false;
    if (key !== "overview") centerTechnology(key);
    techButtons.forEach(button=>{
      const active=button.dataset.techSelect===key;
      button.classList.toggle("active",active);
      button.setAttribute("aria-expanded",String(active));
      button.setAttribute("aria-pressed",String(active));
    });
    overviewTrigger.classList.toggle("active",key==="overview");
    overviewTrigger.setAttribute("aria-expanded",String(key==="overview"));
    const type=document.getElementById("tech-map-type");
    const title=document.getElementById("tech-map-title");
    const description=document.getElementById("tech-map-description");
    const tags=document.getElementById("tech-map-tags");
    if(type)type.textContent=data.type;
    if(title)title.textContent=data.title;
    if(description)description.textContent=data.description;
    if(tags){
      tags.replaceChildren();
      data.tags.forEach(tag=>{
        const chip=document.createElement("span");
        chip.className="tech-map-tag";
        chip.textContent=tag;
        tags.append(chip);
      });
    }
    drawView(data);
    if(shouldScroll)requestAnimationFrame(()=>explorer.scrollIntoView({behavior:reducedMotion?"auto":"smooth",block:"center"}));
  }
  techButtons.forEach(button=>button.addEventListener("click",()=>selectTechnology(button.dataset.techSelect,true)));
  overviewTrigger.addEventListener("click",()=>selectTechnology("overview",true));
  if(closeButton)closeButton.addEventListener("click",()=>{
    explorer.hidden=true;
    overviewTrigger.setAttribute("aria-expanded","false");
    techButtons.forEach(button=>button.setAttribute("aria-expanded","false"));
    overviewTrigger.focus();
  });
})(); 

/* Subtle section entrance and ambient parallax. Kept gentle and disabled for reduced-motion users. */
(() => {
  if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;
  const targets=Array.from(document.querySelectorAll(".section-block, .proof-strip, .contact-section"));
  if ("IntersectionObserver" in window) {
    document.documentElement.classList.add("has-scroll-motion");
    const observer=new IntersectionObserver(entries=>{
      entries.forEach(entry=>{
        if(entry.isIntersecting){
          entry.target.classList.add("is-visible");
          observer.unobserve(entry.target);
        }
      });
    },{threshold:0.08,rootMargin:"0px 0px -4% 0px"});
    targets.forEach(target=>{
      target.classList.add("scroll-reveal");
      observer.observe(target);
    });
  } else {
    targets.forEach(target=>target.classList.add("is-visible"));
  }
  const ambientOne=document.querySelector(".ambient-one");
  const ambientTwo=document.querySelector(".ambient-two");
  let framePending=false;
  const updateParallax=()=>{
    const offset=window.scrollY || window.pageYOffset || 0;
    if(ambientOne)ambientOne.style.transform="translate3d(0,"+Math.min(offset*0.09,170)+"px,0)";
    if(ambientTwo)ambientTwo.style.transform="translate3d(0,"+Math.min(offset*0.045,110)+"px,0)";
    framePending=false;
  };
  window.addEventListener("scroll",()=>{
    if(!framePending){
      framePending=true;
      window.requestAnimationFrame(updateParallax);
    }
  },{passive:true});
  updateParallax();
})();


/* Skills grid: show category titles first, then reveal technology chips on hover, focus or tap. */
(() => {
  const cards = Array.from(document.querySelectorAll(".skill-card"));
  if (!cards.length) return;

  const setOpen = (card, open) => {
    const button = card.querySelector(".skill-card-trigger");
    const panel = card.querySelector(".skill-tools");
    card.classList.toggle("is-open", open);
    if (button) button.setAttribute("aria-expanded", String(open));
    if (panel) panel.setAttribute("aria-hidden", String(!open));
  };

  cards.forEach((card) => {
    const button = card.querySelector(".skill-card-trigger");
    const panel = card.querySelector(".skill-tools");
    if (!button || !panel) return;

    card.addEventListener("pointerenter", (event) => {
      if (event.pointerType === "mouse") panel.setAttribute("aria-hidden", "false");
    });
    card.addEventListener("pointerleave", () => {
      if (!card.classList.contains("is-open") && !card.contains(document.activeElement)) {
        panel.setAttribute("aria-hidden", "true");
      }
    });
    card.addEventListener("focusin", () => panel.setAttribute("aria-hidden", "false"));
    card.addEventListener("focusout", () => {
      window.setTimeout(() => {
        if (!card.classList.contains("is-open") && !card.contains(document.activeElement)) {
          panel.setAttribute("aria-hidden", "true");
        }
      }, 0);
    });

    button.addEventListener("click", () => {
      const willOpen = !card.classList.contains("is-open");
      cards.forEach((other) => setOpen(other, false));
      setOpen(card, willOpen);
    });
  });

  document.addEventListener("click", (event) => {
    if (event.target.closest(".skill-card")) return;
    cards.forEach((card) => setOpen(card, false));
  });

  document.addEventListener("keydown", (event) => {
    if (event.key !== "Escape") return;
    const openCard = cards.find((card) => card.classList.contains("is-open"));
    if (openCard) {
      setOpen(openCard, false);
      const button = openCard.querySelector(".skill-card-trigger");
      if (button) button.focus();
    }
  });
})();
