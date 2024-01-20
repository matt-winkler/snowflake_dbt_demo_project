{% macro show_model_config(model, graph) %}

   {# do log('showing depends on', info=true) #}
   {# do log(model.depends_on) #}
   
   {# map all the nodes and their tests in the graph#}
   {% set graph_nodes = graph['nodes'] %}
   {% set graph_tests = {} %}
   {% do log(graph, info=true) %}
   {% for graph_node in graph_nodes %}
     {% do log(graph_node, info=true) %}
     {% if graph_node.resource_type == 'test' %}
       
       {% set test_parent_nodes = graph_node.depends_on %}
     {% endif %}
   {% endfor %}
   
   {# identify the tests applied to the parent nodes for this model #}
   {% set parent_nodes = model.depends_on['nodes'] %}
   {% for parent_node in parent_nodes %}
      {# set node_reference = ref(dependent_node.split('.')[-1]) #}
      {# do log(node_reference, info=true) #}
      {% set parent_node_info = graph_nodes[parent_node] %}
      {% do log(parent_node_macro_dependencies, info=true) %}

   {% endfor %}

{% endmacro %}